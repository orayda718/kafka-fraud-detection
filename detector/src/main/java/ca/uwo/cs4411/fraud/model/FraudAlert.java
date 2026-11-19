package ca.uwo.cs4411.fraud.model;

import java.util.List;
import java.util.UUID;

/**
 * An alert on the {@code fraud-alerts} topic (key = cardId).
 *
 * {@code paymentIds} lists the payment(s) that triggered the rule, so the
 * verification scripts can match alerts back to injected fraud events.
 */
public record FraudAlert(
        String alertId,
        String cardId,
        Rule rule,
        String reason,
        List<String> paymentIds,
        long detectedAtMs) {

    public FraudAlert {
        paymentIds = paymentIds == null ? List.of() : List.copyOf(paymentIds);
    }

    /** Creates an alert with a random id, detected now (wall-clock time, for latency measurement). */
    public static FraudAlert of(String cardId, Rule rule, String reason, List<String> paymentIds) {
        return new FraudAlert(UUID.randomUUID().toString(), cardId, rule, reason,
                paymentIds, System.currentTimeMillis());
    }
}
