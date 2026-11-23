package ca.uwo.cs4411.fraud.rules;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.serde.JsonSerde;

/**
 * GEO_MISMATCH: two payments on one card from different countries within T minutes
 * (default 10).
 *
 * How it works: a custom processor keeps the last {country, timestamp, paymentId}
 * seen for each card in a key-value state store. Each new payment is compared with
 * that entry; if the country differs and the event-time gap is within the window,
 * an alert is emitted. The store is then updated, so a card that stays in the new
 * country does not alert again.
 *
 * The store is persistent and logged, so Kafka Streams backs it with a changelog
 * topic ({@code <application.id>-geo-last-seen-changelog}) and restores it after a
 * restart. Payments are already keyed by card_id, so no repartitioning is needed.
 */
public class GeoMismatchRule implements FraudRule {

    public static final String STORE = "geo-last-seen";

    /** What we remember about a card's most recent payment. */
    public record LastSeen(String country, long timestampMs, String paymentId) {
    }

    @Override
    public String name() {
        return "geo-mismatch";
    }

    @Override
    public KStream<String, FraudAlert> detect(RuleInputs in) {
        long windowMs = in.config().geoWindow().toMillis();
        return in.payments().processValues(new Supplier(windowMs), Named.as("geo-mismatch-check"));
    }

    static final class Supplier implements FixedKeyProcessorSupplier<String, Payment, FraudAlert> {
        private final long windowMs;

        Supplier(long windowMs) {
            this.windowMs = windowMs;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(STORE),
                    Serdes.String(),
                    new JsonSerde<>(LastSeen.class)));
        }

        @Override
        public FixedKeyProcessor<String, Payment, FraudAlert> get() {
            return new FixedKeyProcessor<>() {
                private FixedKeyProcessorContext<String, FraudAlert> context;
                private KeyValueStore<String, LastSeen> lastSeen;

                @Override
                public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
                    this.context = context;
                    this.lastSeen = context.getStateStore(STORE);
                }

                @Override
                public void process(FixedKeyRecord<String, Payment> record) {
                    Payment p = record.value();
                    if (p == null || p.country() == null) {
                        return;
                    }
                    String card = record.key();
                    LastSeen prev = lastSeen.get(card);

                    if (prev != null
                            && !prev.country().equals(p.country())
                            && Math.abs(p.timestampMs() - prev.timestampMs()) <= windowMs) {
                        String reason = String.format("payments from %s and %s within %d minutes",
                                prev.country(), p.country(), windowMs / 60_000);
                        context.forward(record.withValue(FraudAlert.of(card, Rule.GEO_MISMATCH, reason,
                                List.of(prev.paymentId(), p.paymentId()))));
                    }

                    // Only move "last seen" forward in event time, so a late payment
                    // does not overwrite a newer one.
                    if (prev == null || p.timestampMs() >= prev.timestampMs()) {
                        lastSeen.put(card, new LastSeen(p.country(), p.timestampMs(), p.paymentId()));
                    }
                }
            };
        }
    }
}