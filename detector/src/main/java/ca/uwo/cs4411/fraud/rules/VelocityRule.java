package ca.uwo.cs4411.fraud.rules;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.serde.JsonSerde;

/**
 * VELOCITY: more than N payments on one card within a time window (default: more than 5 in 60 s).
 *
 * How it works:
 * <ol>
 *   <li>Group payments by card and count them in <b>sliding windows</b> of the
 *       configured size, keeping the payment ids. Sliding windows cover every
 *       60-second span, so a burst that straddles a minute boundary is still
 *       caught (fixed "tumbling" minute windows would split it and miss it).</li>
 *   <li>Keep windows whose count exceeds the limit.</li>
 *   <li>One burst falls in many overlapping sliding windows, so a small state
 *       store remembers, per card, the end of the last window that raised an
 *       alert. Windows overlapping it belong to the same burst and are
 *       suppressed, giving exactly one alert per burst.</li>
 * </ol>
 *
 * Windows use event time (payment timestamp_ms) and accept payments that arrive
 * up to {@code velocity.grace.seconds} late. Both stores are backed by changelog
 * topics, so counts survive a detector restart.
 */
public class VelocityRule implements FraudRule {

    public static final String COUNTS_STORE = "velocity-counts";
    public static final String LAST_ALERT_STORE = "velocity-last-alert";

    /** Running count and payment ids for one card in one window. */
    public record WindowAgg(long count, List<String> paymentIds) {
        public WindowAgg {
            paymentIds = paymentIds == null ? List.of() : List.copyOf(paymentIds);
        }

        static WindowAgg empty() {
            return new WindowAgg(0, List.of());
        }

        WindowAgg add(String paymentId) {
            List<String> ids = new ArrayList<>(paymentIds);
            ids.add(paymentId);
            return new WindowAgg(count + 1, ids);
        }
    }

    /** A window over the limit: its bounds (epoch ms) and contents. */
    record Breach(long windowStart, long windowEnd, WindowAgg agg) {
    }

    @Override
    public String name() {
        return "velocity";
    }

    @Override
    public KStream<String, FraudAlert> detect(RuleInputs in) {
        int limit = in.config().velocityMaxCount();
        Duration window = in.config().velocityWindow();
        Duration grace = in.config().velocityGrace();
        JsonSerde<WindowAgg> aggSerde = new JsonSerde<>(WindowAgg.class);

        return in.payments()
                .groupByKey(Grouped.as("velocity-group"))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(window, grace))
                .aggregate(WindowAgg::empty,
                        (card, payment, agg) -> agg.add(payment.paymentId()),
                        Named.as("velocity-aggregate"),
                        Materialized.<String, WindowAgg, WindowStore<Bytes, byte[]>>as(COUNTS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggSerde)
                                // Forward every update so the first breach is never coalesced away.
                                .withCachingDisabled())
                .toStream(Named.as("velocity-windows"))
                .filter((windowedCard, agg) -> agg != null && agg.count() > limit,
                        Named.as("velocity-over-limit"))
                .map((windowedCard, agg) -> KeyValue.pair(windowedCard.key(),
                        new Breach(windowedCard.window().start(), windowedCard.window().end(), agg)),
                        Named.as("velocity-unwrap-window"))
                .processValues(new OneAlertPerBurst(limit, window), Named.as("velocity-dedup"));
    }

    /** Emits an alert for the first breaching window of each burst, suppresses overlapping ones. */
    static final class OneAlertPerBurst implements FixedKeyProcessorSupplier<String, Breach, FraudAlert> {
        private final int limit;
        private final Duration window;

        OneAlertPerBurst(int limit, Duration window) {
            this.limit = limit;
            this.window = window;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(LAST_ALERT_STORE), Serdes.String(), Serdes.Long()));
        }

        @Override
        public FixedKeyProcessor<String, Breach, FraudAlert> get() {
            return new FixedKeyProcessor<>() {
                private FixedKeyProcessorContext<String, FraudAlert> context;
                private KeyValueStore<String, Long> lastAlertEnd;

                @Override
                public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
                    this.context = context;
                    this.lastAlertEnd = context.getStateStore(LAST_ALERT_STORE);
                }

                @Override
                public void process(FixedKeyRecord<String, Breach> record) {
                    String card = record.key();
                    Breach b = record.value();
                    Long prevEnd = lastAlertEnd.get(card);
                    boolean sameBurst = prevEnd != null && b.windowStart() <= prevEnd;
                    // Extend the remembered end so a long-running burst stays one alert.
                    lastAlertEnd.put(card, prevEnd == null ? b.windowEnd() : Math.max(prevEnd, b.windowEnd()));
                    if (sameBurst) {
                        return;
                    }
                    String reason = String.format("%d transactions in %d seconds (limit %d)",
                            b.agg().count(), window.toSeconds(), limit);
                    context.forward(record.withValue(
                            FraudAlert.of(card, Rule.VELOCITY, reason, b.agg().paymentIds())));
                }
            };
        }
    }
}
