package ca.uwo.cs4411.fraud.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import ca.uwo.cs4411.fraud.model.Payment;

class PaymentTimestampExtractorTest {

    private final PaymentTimestampExtractor extractor = new PaymentTimestampExtractor();

    private static ConsumerRecord<Object, Object> record(Object value, long kafkaTs) {
        return new ConsumerRecord<>("payments", 0, 0L, kafkaTs, TimestampType.CREATE_TIME,
                -1, -1, "card_1", value, new RecordHeaders(), Optional.empty());
    }

    private static Payment payment(long ts) {
        return new Payment("p1", "card_1", 10.0, "CAD", "m", "food", "CA", ts);
    }

    @Test
    void usesPaymentEventTime() {
        assertEquals(1_000L, extractor.extract(record(payment(1_000L), 9_999L), 5_000L));
    }

    @Test
    void fallsBackToKafkaTimestampWhenEventTimeMissing() {
        assertEquals(9_999L, extractor.extract(record(payment(0L), 9_999L), 5_000L));
        assertEquals(9_999L, extractor.extract(record("not a payment", 9_999L), 5_000L));
    }

    @Test
    void fallsBackToPartitionTimeWhenNothingValid() {
        assertEquals(5_000L, extractor.extract(record(null, -1L), 5_000L));
    }
}
