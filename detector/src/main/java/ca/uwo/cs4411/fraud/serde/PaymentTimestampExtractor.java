package ca.uwo.cs4411.fraud.serde;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import ca.uwo.cs4411.fraud.model.Payment;

/**
 * Makes Kafka Streams use event time (when the payment happened) instead of
 * the time the record was produced or arrived at the broker.
 *
 * Windows (VELOCITY) and time comparisons (GEO_MISMATCH) therefore follow the
 * payment's own {@code timestamp_ms}, so late or replayed records still land in
 * the correct window.
 *
 * Fallbacks, so a bad record is never dropped for having an invalid timestamp:
 * the record's Kafka timestamp, then the highest timestamp seen on the partition.
 */
public class PaymentTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof Payment p && p.timestampMs() > 0) {
            return p.timestampMs();
        }
        if (record.timestamp() >= 0) {
            return record.timestamp();
        }
        return Math.max(partitionTime, 0);
    }
}
