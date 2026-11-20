package ca.uwo.cs4411.fraud.rules;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Rule;
import ca.uwo.cs4411.fraud.serde.AppSerdes;

/**
 * OVER_LIMIT: a payment above the card's spending limit.
 *
 * A stream-table join: each payment is looked up against the current
 * {@code card-profiles} table entry for the same card_id. The join is local
 * (no network call, no repartitioning) because both topics are keyed by
 * card_id with the same partition count, so a card's payments and its profile
 * always land in the same task. Profile updates on the compacted topic are
 * picked up automatically; payments for cards with no profile are skipped.
 */
public class OverLimitRule implements FraudRule {

    @Override
    public String name() {
        return "over-limit";
    }

    @Override
    public KStream<String, FraudAlert> detect(RuleInputs in) {
        return in.payments()
                .join(in.profiles(),
                        (payment, profile) -> payment.amount() > profile.spendingLimit()
                                ? Optional.of(FraudAlert.of(payment.cardId(), Rule.OVER_LIMIT,
                                        String.format(Locale.ROOT, "amount %.2f exceeds spending limit %.2f",
                                                payment.amount(), profile.spendingLimit()),
                                        List.of(payment.paymentId())))
                                : Optional.<FraudAlert>empty(),
                        Joined.with(Serdes.String(), AppSerdes.PAYMENT, AppSerdes.CARD_PROFILE)
                                .withName("over-limit-join"))
                .flatMapValues(alert -> alert.map(List::of).orElse(List.of()),
                        Named.as("over-limit-alerts"));
    }
}
