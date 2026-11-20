package ca.uwo.cs4411.fraud.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class RulesConfigTest {

    @Test
    void bundledDefaultsMatchContextDoc() {
        RulesConfig c = RulesConfig.fromProperties(RulesConfig.loadDefaults());
        assertEquals(5, c.velocityMaxCount());
        assertEquals(Duration.ofMinutes(1), c.velocityWindow());
        assertEquals(Duration.ofSeconds(10), c.velocityGrace());
        assertEquals(Duration.ofMinutes(10), c.geoWindow());
        assertEquals(5.0, c.spikeMultiplier());
        assertEquals(5, c.spikeMinPayments());
    }

    @Test
    void overrideReplacesOnlyGivenKeys() {
        Properties p = RulesConfig.loadDefaults();
        p.setProperty("velocity.max.count", "3");
        RulesConfig c = RulesConfig.fromProperties(p);
        assertEquals(3, c.velocityMaxCount());
        assertEquals(Duration.ofMinutes(10), c.geoWindow());
    }

    @Test
    void missingKeyIsRejected() {
        Properties p = RulesConfig.loadDefaults();
        p.remove("geo.window.minutes");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RulesConfig.fromProperties(p));
        assertTrue(e.getMessage().contains("geo.window.minutes"));
    }

    @Test
    void invalidValuesAreRejected() {
        Properties p = RulesConfig.loadDefaults();
        p.setProperty("velocity.max.count", "0");
        assertThrows(IllegalArgumentException.class, () -> RulesConfig.fromProperties(p));

        Properties q = RulesConfig.loadDefaults();
        q.setProperty("spike.multiplier", "abc");
        assertThrows(IllegalArgumentException.class, () -> RulesConfig.fromProperties(q));
    }
}
