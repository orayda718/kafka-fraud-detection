package ca.uwo.cs4411.fraud.rules;

import java.util.List;

/**
 * The rules the detector runs. Add one line here per rule class.
 *
 * Rules (see rules.properties for thresholds):
 *   VELOCITY      - VelocityRule      (detector-core)
 *   OVER_LIMIT    - OverLimitRule     (detector-core)
 *   GEO_MISMATCH  - GeoMismatchRule   (detector-state)
 *   AMOUNT_SPIKE  - AmountSpikeRule   (detector-state)
 */
public final class FraudRules {

    private FraudRules() {
    }

    public static List<FraudRule> all() {
        return List.of(
                new VelocityRule()
                // more rules get added here as they are implemented
        );
    }
}
