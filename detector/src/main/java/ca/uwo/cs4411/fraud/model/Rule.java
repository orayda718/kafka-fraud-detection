package ca.uwo.cs4411.fraud.model;

/** The fraud rules. Serialized to JSON by name, e.g. {@code "VELOCITY"}. */
public enum Rule {
    VELOCITY,
    GEO_MISMATCH,
    AMOUNT_SPIKE,
    OVER_LIMIT
}
