package ca.uwo.cs4411.fraud.rules;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import ca.uwo.cs4411.fraud.FraudTopology;
import ca.uwo.cs4411.fraud.Topics;
import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.serde.AppSerdes;

/** Runs the real topology with the given rules in memory, for rule tests. */
final class RuleTestDriver implements AutoCloseable {

    final TopologyTestDriver driver;
    final TestInputTopic<String, Payment> payments;
    final TestInputTopic<String, CardProfile> profiles;
    final TestOutputTopic<String, FraudAlert> alerts;

    RuleTestDriver(FraudRule... rules) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "rule-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        p.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("java.io.tmpdir") + "/rule-test-" + System.nanoTime());
        RulesConfig config = RulesConfig.fromProperties(RulesConfig.loadDefaults());
        driver = new TopologyTestDriver(FraudTopology.build(config, List.of(rules), p), p);
        payments = driver.createInputTopic(Topics.PAYMENTS, new StringSerializer(),
                AppSerdes.PAYMENT.serializer());
        profiles = driver.createInputTopic(Topics.CARD_PROFILES, new StringSerializer(),
                AppSerdes.CARD_PROFILE.serializer());
        alerts = driver.createOutputTopic(Topics.FRAUD_ALERTS, new StringDeserializer(),
                AppSerdes.FRAUD_ALERT.deserializer());
    }

    /** Sends a payment with the given id, card, amount, country and event time (seconds). */
    void pay(String id, String card, double amount, String country, double atSeconds) {
        long ts = (long) (atSeconds * 1000);
        payments.pipeInput(card, new Payment(id, card, amount, "CAD", "Shop", "retail", country, ts), ts);
    }

    void profile(String card, String homeCountry, double limit) {
        profiles.pipeInput(card, new CardProfile(card, homeCountry, limit), 0L);
    }

    List<FraudAlert> readAlerts() {
        return alerts.readValuesToList();
    }

    @Override
    public void close() {
        driver.close();
    }
}
