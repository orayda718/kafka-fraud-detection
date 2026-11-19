package ca.uwo.cs4411.fraud.serde;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic JSON serde backed by Jackson.
 *
 * Java camelCase fields map to snake_case JSON (cardId -> card_id), matching
 * what the Python simulator produces. Unknown JSON fields are ignored so the
 * simulator can add fields without breaking the detector. A null payload
 * (e.g. a tombstone on the compacted card-profiles topic) maps to null.
 */
public class JsonSerde<T> implements Serde<T> {

    static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> {
            if (value == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (IOException e) {
                throw new SerializationException("Failed to serialize " + type.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (IOException e) {
                throw new SerializationException(
                        "Failed to deserialize " + type.getSimpleName() + " from topic " + topic, e);
            }
        };
    }
}
