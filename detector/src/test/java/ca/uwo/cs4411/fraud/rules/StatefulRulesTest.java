package ca.uwo.cs4411.fraud.rules;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ca.uwo.cs4411.fraud.FraudTopology;
import ca.uwo.cs4411.fraud.Topics;
import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.serde.AppSerdes;

/** Tests for GEO_MISMATCH and AMOUNT_SPIKE using the full topology with only these two rules. */
class StatefulRulesTest {

    private static final long T0 = 1_795_194_000_000L;
    private static final long MIN = 60_000L;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Payment> payments;
    private TestOutputTopic<String, FraudAlert> alerts;
    private int seq;

    @BeforeEach
    void setUp() throws IOException {
        RulesConfig cfg = new RulesConfig(5, Duration.ofSeconds(60), Duration.ofSeconds(10),
                Duration.ofMinutes(10), 5.0, 5);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstreams-test").toString());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        driver = new TopologyTestDriver(
                FraudTopology.build(cfg, List.of(new GeoMismatchRule(), new AmountSpikeRule()), props), props);
        payments = driver.createInputTopic(Topics.PAYMENTS,
                Serdes.String().serializer(), AppSerdes.PAYMENT.serializer());
        alerts = driver.createOutputTopic(Topics.FRAUD_ALERTS,
                Serdes.String().deserializer(), AppSerdes.FRAUD_ALERT.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void send(String card, double amount, String country, long ts) {
        Payment p = new Payment("pay-" + (++seq), card, amount, "CAD", "Shop", "food", country, ts);
        payments.pipeInput(card, p, Instant.ofEpochMilli(ts));
    }

    // ---- GEO_MISMATCH ----

    @Test
    void geoMismatch_differentCountryWithinWindow_alerts() {
        send("card_1", 20, "CA", T0);
        send("card_1", 20, "BR", T0 + 2 * MIN);

        List<FraudAlert> out = alerts.readValuesToList();
        assertEquals(1, out.size());
        assertEquals(Rule.GEO_MISMATCH, out.get(0).rule());
        assertEquals(List.of("pay-1", "pay-2"), out.get(0).paymentIds());
    }

    @Test
    void geoMismatch_differentCountryOutsideWindow_noAlert() {
        send("card_1", 20, "CA", T0);
        send("card_1", 20, "BR", T0 + 30 * MIN);
        assertTrue(alerts.isEmpty());
    }

    @Test
    void geoMismatch_sameCountry_noAlert() {
        send("card_1", 20, "CA", T0);
        send("card_1", 20, "CA", T0 + 2 * MIN);
        assertTrue(alerts.isEmpty());
    }

    @Test
    void geoMismatch_isTrackedPerCard() {
        send("card_1", 20, "CA", T0);
        send("card_2", 20, "BR", T0 + MIN);
        assertTrue(alerts.isEmpty());
    }

    // ---- AMOUNT_SPIKE ----

    @Test
    void amountSpike_afterEnoughHistory_alerts() {
        for (int i = 0; i < 5; i++) {
            send("card_1", 50, "CA", T0 + i * MIN);
        }
        send("card_1", 500, "CA", T0 + 6 * MIN);

        List<FraudAlert> out = alerts.readValuesToList();
        assertEquals(1, out.size());
        assertEquals(Rule.AMOUNT_SPIKE, out.get(0).rule());
        assertEquals(List.of("pay-6"), out.get(0).paymentIds());
    }

    @Test
    void amountSpike_tooFewPriorPayments_noAlert() {
        for (int i = 0; i < 3; i++) {
            send("card_1", 50, "CA", T0 + i * MIN);
        }
        send("card_1", 500, "CA", T0 + 4 * MIN);
        assertTrue(alerts.isEmpty());
    }

    @Test
    void amountSpike_belowMultiplier_noAlert() {
        for (int i = 0; i < 5; i++) {
            send("card_1", 50, "CA", T0 + i * MIN);
        }
        send("card_1", 200, "CA", T0 + 6 * MIN); // 4x the average, limit is 5x
        assertTrue(alerts.isEmpty());
    }
}