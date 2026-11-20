package ca.uwo.cs4411.fraud;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.rules.FraudRule;
import ca.uwo.cs4411.fraud.rules.FraudRules;
import ca.uwo.cs4411.fraud.rules.RuleInputs;
import ca.uwo.cs4411.fraud.serde.AppSerdes;

/** Checks the topology wiring with simple test-only rules, no Kafka cluster needed. */
class FraudTopologyTest {

    /** Alerts on any payment above 100, using only the payment stream. */
    static final FraudRule BIG_PAYMENT = new FraudRule() {
        public String name() {
            return "big-payment";
        }

        public KStream<String, FraudAlert> detect(RuleInputs in) {
            return in.payments()
                    .filter((card, p) -> p.amount() > 100)
                    .mapValues(p -> FraudAlert.of(p.cardId(), Rule.VELOCITY, "big",
                            List.of(p.paymentId())));
        }
    };

    /** Alerts on payments made outside the card's home country, using the profiles table. */
    static final FraudRule ABROAD = new FraudRule() {
        public String name() {
            return "abroad";
        }

        public KStream<String, FraudAlert> detect(RuleInputs in) {
            return in.payments()
                    .join(in.profiles(), (p, prof) -> prof.homeCountry().equals(p.country()) ? null
                            : FraudAlert.of(p.cardId(), Rule.GEO_MISMATCH, "abroad", List.of(p.paymentId())),
                            Joined.with(Serdes.String(), AppSerdes.PAYMENT, AppSerdes.CARD_PROFILE))
                    .filter((card, alert) -> alert != null);
        }
    };

    private TopologyTestDriver driver;
    private TestInputTopic<String, Payment> payments;
    private TestInputTopic<String, CardProfile> profiles;
    private TestOutputTopic<String, FraudAlert> alerts;

    private static Properties props() {
        Properties p = FraudDetectorApp.config();
        p.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("java.io.tmpdir") + "/fraud-topology-test-" + System.nanoTime());
        return p;
    }

    private void start(List<FraudRule> rules) {
        Properties p = props();
        Topology t = FraudTopology.build(RulesConfig.fromProperties(RulesConfig.loadDefaults()), rules, p);
        driver = new TopologyTestDriver(t, p);
        payments = driver.createInputTopic(Topics.PAYMENTS, new StringSerializer(),
                AppSerdes.PAYMENT.serializer());
        profiles = driver.createInputTopic(Topics.CARD_PROFILES, new StringSerializer(),
                AppSerdes.CARD_PROFILE.serializer());
        alerts = driver.createOutputTopic(Topics.FRAUD_ALERTS, new StringDeserializer(),
                AppSerdes.FRAUD_ALERT.deserializer());
    }

    private static Payment pay(String id, String card, double amount, String country, long ts) {
        return new Payment(id, card, amount, "CAD", "Shop", "retail", country, ts);
    }

    @AfterEach
    void close() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void alertsFromAllRulesAreMergedIntoFraudAlerts() {
        start(List.of(BIG_PAYMENT, ABROAD));
        profiles.pipeInput("card_1", new CardProfile("card_1", "CA", 2000));

        payments.pipeInput("card_1", pay("p1", "card_1", 20, "CA", 1_000));   // no alert
        payments.pipeInput("card_1", pay("p2", "card_1", 500, "CA", 2_000));  // big
        payments.pipeInput("card_1", pay("p3", "card_1", 20, "BR", 3_000));   // abroad

        List<KeyValue<String, FraudAlert>> out = alerts.readKeyValuesToList();
        assertEquals(2, out.size());
        assertTrue(out.stream().allMatch(kv -> kv.key.equals("card_1")));
        assertEquals(List.of("p2"), out.get(0).value.paymentIds());
        assertEquals(Rule.VELOCITY, out.get(0).value.rule());
        assertEquals(List.of("p3"), out.get(1).value.paymentIds());
        assertEquals(Rule.GEO_MISMATCH, out.get(1).value.rule());
    }

    @Test
    void malformedPaymentIsSkippedNotFatal() {
        start(List.of(BIG_PAYMENT));
        TestInputTopic<String, String> raw = driver.createInputTopic(Topics.PAYMENTS,
                new StringSerializer(), new StringSerializer());
        raw.pipeInput("card_1", "not json");
        payments.pipeInput("card_1", pay("p1", "card_1", 500, "CA", 1_000));
        assertEquals(1, alerts.readValuesToList().size());
    }

    @Test
    void runsWithTheRegisteredRules() {
        assertDoesNotThrow(() -> start(FraudRules.all()));
        profiles.pipeInput("card_1", new CardProfile("card_1", "CA", 2000));
        payments.pipeInput("card_1", pay("p1", "card_1", 20, "CA", 1_000));
    }
}
