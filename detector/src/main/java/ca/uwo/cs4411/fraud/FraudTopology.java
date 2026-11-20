package ca.uwo.cs4411.fraud;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.rules.FraudRule;
import ca.uwo.cs4411.fraud.rules.RuleInputs;
import ca.uwo.cs4411.fraud.serde.AppSerdes;
import ca.uwo.cs4411.fraud.serde.PaymentTimestampExtractor;

/**
 * Builds the detector's processing graph:
 *
 * <pre>
 *   payments ──(event time)──┬─▶ rule 1 ─┐
 *                            ├─▶ rule 2 ─┼─▶ merge ─▶ fraud-alerts
 *   card-profiles (KTable) ──┴─▶ ...    ─┘
 * </pre>
 *
 * card-profiles is read as a KTable (not a GlobalKTable): each instance only
 * holds the profiles for the partitions it processes. This works because
 * payments and card-profiles are both keyed by card_id and have the same
 * partition count (co-partitioned). With topology optimization on, the
 * compacted card-profiles topic itself is the table's changelog, so no extra
 * changelog topic is created.
 */
public final class FraudTopology {

    public static final String PROFILES_STORE = "card-profiles-store";

    private FraudTopology() {
    }

    public static Topology build(RulesConfig config, List<FraudRule> rules, Properties streamsProps) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Payment> payments = builder.stream(Topics.PAYMENTS,
                Consumed.with(Serdes.String(), AppSerdes.PAYMENT)
                        .withTimestampExtractor(new PaymentTimestampExtractor())
                        .withName("payments-source"));

        KTable<String, CardProfile> profiles = builder.table(Topics.CARD_PROFILES,
                Consumed.with(Serdes.String(), AppSerdes.CARD_PROFILE).withName("card-profiles-source"),
                Materialized.as(PROFILES_STORE));

        RuleInputs inputs = new RuleInputs(builder, payments, profiles, config);

        KStream<String, FraudAlert> alerts = null;
        for (FraudRule rule : rules) {
            KStream<String, FraudAlert> ruleAlerts = rule.detect(inputs);
            alerts = alerts == null ? ruleAlerts : alerts.merge(ruleAlerts);
        }
        if (alerts != null) {
            alerts.to(Topics.FRAUD_ALERTS, Produced.with(Serdes.String(), AppSerdes.FRAUD_ALERT)
                    .withName("fraud-alerts-sink"));
        }

        // Passing the config lets Streams apply topology optimizations (source-topic reuse).
        return builder.build(streamsProps);
    }
}
