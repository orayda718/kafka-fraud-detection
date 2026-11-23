package ca.uwo.cs4411.fraud.model;

/**
 * A card payment on the {@code payments} topic (key = cardId).
 *
 * JSON uses snake_case field names, e.g. {@code "card_id"}, {@code "timestamp_ms"}.
 * {@code timestampMs} is the event time in epoch milliseconds and is what
 * Kafka Streams uses for windowing (see PaymentTimestampExtractor).
 */
public record Payment(
        String paymentId,
        String cardId,
        double amount,
        String currency,
        String merchant,
        String merchantCategory,
        String country,
        long timestampMs) {
}
