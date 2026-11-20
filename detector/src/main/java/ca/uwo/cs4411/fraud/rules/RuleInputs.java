package ca.uwo.cs4411.fraud.rules;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import ca.uwo.cs4411.fraud.config.RulesConfig;
import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.Payment;

/**
 * Everything a rule can use to build its part of the topology.
 *
 * @param builder  for rules that need to register their own state stores
 * @param payments payments keyed by card_id, timestamped by event time
 * @param profiles card profiles keyed by card_id (co-partitioned with payments)
 * @param config   rule thresholds from rules.properties
 */
public record RuleInputs(
        StreamsBuilder builder,
        KStream<String, Payment> payments,
        KTable<String, CardProfile> profiles,
        RulesConfig config) {
}
