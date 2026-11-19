package ca.uwo.cs4411.fraud.model;

/** Reference data on the log-compacted {@code card-profiles} topic (key = cardId). */
public record CardProfile(
        String cardId,
        String homeCountry,
        double spendingLimit) {
}
