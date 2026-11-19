package ca.uwo.cs4411.fraud;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

/**
 * Entry point for the fraud detector.
 *
 * Skeleton only: it reads `payments` and logs each event, to prove the
 * Streams app connects to the cluster. The fraud rules (windowed count,
 * country change, amount vs. profile join) get added in buildTopology().
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
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return p;
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("payments", Consumed.with(Serdes.String(), Serdes.String()))
               .peek((card, json) -> System.out.println("payment " + card + " -> " + json));
        // TODO: join with card-profiles KTable, apply rules, write to fraud-alerts
        return builder.build();
    }

    public static void main(String[] args) {
        Topology topology = buildTopology();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, config());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
