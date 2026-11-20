package ca.uwo.cs4411.fraud.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

/**
 * Fraud rule thresholds, loaded from {@code rules.properties}.
 *
 * Defaults come from the copy bundled in the jar. If the {@code RULES_FILE}
 * environment variable points to a file, its keys override the defaults, so
 * thresholds can be tuned without recompiling.
 */
public record RulesConfig(
        int velocityMaxCount,
        Duration velocityWindow,
        Duration velocityGrace,
        Duration geoWindow,
        double spikeMultiplier,
        int spikeMinPayments) {

    public static final String RESOURCE = "/rules.properties";
    public static final String OVERRIDE_ENV = "RULES_FILE";

    public RulesConfig {
        requirePositive("velocity.max.count", velocityMaxCount);
        requirePositive("velocity.window.seconds", velocityWindow.toSeconds());
        if (velocityGrace.isNegative()) {
            throw new IllegalArgumentException("velocity.grace.seconds must be >= 0");
        }
        requirePositive("geo.window.minutes", geoWindow.toMinutes());
        if (!(spikeMultiplier > 1.0)) {
            throw new IllegalArgumentException("spike.multiplier must be > 1.0, got " + spikeMultiplier);
        }
        requirePositive("spike.min.payments", spikeMinPayments);
    }

    /** Loads bundled defaults, then applies the file named by RULES_FILE if set. */
    public static RulesConfig load() {
        Properties props = loadDefaults();
        String override = System.getenv(OVERRIDE_ENV);
        if (override != null && !override.isBlank()) {
            try (Reader r = Files.newBufferedReader(Path.of(override))) {
                props.load(r);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read " + OVERRIDE_ENV + "=" + override, e);
            }
        }
        return fromProperties(props);
    }

    public static Properties loadDefaults() {
        Properties props = new Properties();
        try (InputStream in = RulesConfig.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(RESOURCE + " not found on classpath");
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return props;
    }

    public static RulesConfig fromProperties(Properties p) {
        return new RulesConfig(
                intProp(p, "velocity.max.count"),
                Duration.ofSeconds(longProp(p, "velocity.window.seconds")),
                Duration.ofSeconds(longProp(p, "velocity.grace.seconds")),
                Duration.ofMinutes(longProp(p, "geo.window.minutes")),
                doubleProp(p, "spike.multiplier"),
                intProp(p, "spike.min.payments"));
    }

    private static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing rule setting: " + key);
        }
        return v.trim();
    }

    private static int intProp(Properties p, String key) {
        try {
            return Integer.parseInt(required(p, key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer", e);
        }
    }

    private static long longProp(Properties p, String key) {
        try {
            return Long.parseLong(required(p, key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer", e);
        }
    }

    private static double doubleProp(Properties p, String key) {
        try {
            return Double.parseDouble(required(p, key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number", e);
        }
    }

    private static void requirePositive(String key, long v) {
        if (v <= 0) {
            throw new IllegalArgumentException(key + " must be > 0, got " + v);
        }
    }
}
