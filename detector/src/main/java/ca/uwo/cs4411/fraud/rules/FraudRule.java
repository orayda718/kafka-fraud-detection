package ca.uwo.cs4411.fraud.rules;

import org.apache.kafka.streams.kstream.KStream;

import ca.uwo.cs4411.fraud.model.FraudAlert;

/**
 * One fraud rule. Each rule lives in its own class, reads from the shared
 * inputs, and returns a stream of alerts keyed by card_id. FraudTopology
 * merges every rule's alerts and writes them to the fraud-alerts topic.
 *
 * To add a rule: implement this interface and register it in FraudRules.
 */
public interface FraudRule {

    /** Short name used in logs and for naming this rule's state stores. */
    String name();

    /** Adds this rule's processing steps and returns the alerts it produces. */
    KStream<String, FraudAlert> detect(RuleInputs inputs);
}
