package ca.uwo.cs4411.fraud;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.rules.FraudRules;

/**
 * Entry point for the fraud detector.
 *
 * Environment variables:
 *   BOOTSTRAP   Kafka bootstrap servers (default: the 3 local brokers)
 *   GUARANTEE   exactly_once_v2 (default) or at_least_once, for the duplicate-alert experiment
 *   RULES_FILE  optional rules.properties overriding the bundled thresholds
 */
public class FraudDetectorApp {

    static Properties config() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detector");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
              System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096"));
        // Switch to AT_LEAST_ONCE for the duplicate-alert comparison experiment.
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
              System.getenv().getOrDefault("GUARANTEE", StreamsConfig.EXACTLY_ONCE_V2));
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3); // internal changelog/repartition topics
        p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // Skip (and log) records that aren't valid JSON instead of crashing the app.
        p.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
              LogAndContinueExceptionHandler.class);
        return p;
    }

    public static void main(String[] args) {
        Properties props = config();
        RulesConfig rules = RulesConfig.load();
        System.out.println("Rules: " + rules);
        System.out.println("Processing guarantee: " + props.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));

        Topology topology = FraudTopology.build(rules, FraudRules.all(), props);
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
