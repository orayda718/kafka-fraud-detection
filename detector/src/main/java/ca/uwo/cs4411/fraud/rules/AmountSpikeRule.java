package ca.uwo.cs4411.fraud.rules;

import java.util.List;
import java.util.Locale;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.serde.JsonSerde;

/**
 * AMOUNT_SPIKE: amount greater than K x the card's running average, once the card
 * has at least M prior payments (defaults K = 5, M = 5).
 *
 * How it works: payments are grouped by card and aggregated into a per-card
 * {count, sum} table (backed by a changelog topic). The check happens inside the
 * aggregator, comparing the new amount with the average of the payments BEFORE it,
 * so the payment being tested never inflates its own baseline. The aggregate also
 * records the verdict for the latest payment, and only updates flagged as spikes
 * are turned into alerts.
 *
 * Caching is disabled on the store so every update is forwarded; with caching on,
 * a spike flag could be overwritten by the next payment before it is emitted.
 */
public class AmountSpikeRule implements FraudRule {

    public static final String STORE = "amount-spike-stats";

    /**
     * Running stats for one card, plus the verdict for its latest payment.
     * (Kept as plain components so the JSON serde can store it.)
     */
    public record CardStats(
            long count,
            double sum,
            boolean spike,
            double avgBefore,
            double lastAmount,
            String lastPaymentId) {

        static CardStats empty() {
            return new CardStats(0, 0.0, false, 0.0, 0.0, null);
        }

        CardStats add(Payment p, double multiplier, int minPayments) {
            double avg = count > 0 ? sum / count : 0.0;
            boolean isSpike = count >= minPayments && avg > 0 && p.amount() > multiplier * avg;
            return new CardStats(count + 1, sum + p.amount(), isSpike, avg, p.amount(), p.paymentId());
        }
    }

    @Override
    public String name() {
        return "amount-spike";
    }

    @Override
    public KStream<String, FraudAlert> detect(RuleInputs in) {
        double multiplier = in.config().spikeMultiplier();
        int minPayments = in.config().spikeMinPayments();

        return in.payments()
                .groupByKey(Grouped.as("spike-group"))
                .aggregate(CardStats::empty,
                        (card, payment, stats) -> stats.add(payment, multiplier, minPayments),
                        Named.as("spike-aggregate"),
                        Materialized.<String, CardStats, KeyValueStore<Bytes, byte[]>>as(STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(CardStats.class))
                                .withCachingDisabled())
                .toStream(Named.as("spike-updates"))
                .filter((card, stats) -> stats != null && stats.spike(), Named.as("spike-only"))
                .mapValues((card, stats) -> FraudAlert.of(card, Rule.AMOUNT_SPIKE,
                                String.format(Locale.ROOT,
                                        "amount %.2f is %.1fx the running average %.2f (limit %.0fx)",
                                        stats.lastAmount(), stats.lastAmount() / stats.avgBefore(),
                                        stats.avgBefore(), multiplier),
                                List.of(stats.lastPaymentId())),
                        Named.as("spike-alerts"));
    }
}