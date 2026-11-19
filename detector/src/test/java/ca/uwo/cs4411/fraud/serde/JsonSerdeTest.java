package ca.uwo.cs4411.fraud.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;
import ca.uwo.cs4411.fraud.model.Rule;

class JsonSerdeTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void deserializesPaymentFromSimulatorJson() {
        String json = """
                {"payment_id":"p1","card_id":"card_0042","amount":54.2,"currency":"CAD",
                 "merchant":"Coffee Shop 12","merchant_category":"food","country":"CA",
                 "timestamp_ms":1795194000123,"extra_field":"ignored"}
                """;
        Payment p = AppSerdes.PAYMENT.deserializer().deserialize("payments", utf8(json));
        assertEquals(new Payment("p1", "card_0042", 54.2, "CAD", "Coffee Shop 12", "food", "CA",
                1795194000123L), p);
    }

    @Test
    void deserializesCardProfile() {
        String json = "{\"card_id\":\"card_0042\",\"home_country\":\"CA\",\"spending_limit\":2000.0}";
        CardProfile c = AppSerdes.CARD_PROFILE.deserializer().deserialize("card-profiles", utf8(json));
        assertEquals(new CardProfile("card_0042", "CA", 2000.0), c);
    }

    @Test
    void fraudAlertRoundTripsWithSnakeCase() {
        FraudAlert a = new FraudAlert("a1", "card_0042", Rule.VELOCITY,
                "7 transactions in 1 minute (limit 5)", List.of("p1", "p2"), 1795194005456L);
        byte[] bytes = AppSerdes.FRAUD_ALERT.serializer().serialize("fraud-alerts", a);
        String json = new String(bytes, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"card_id\":\"card_0042\""), json);
        assertTrue(json.contains("\"rule\":\"VELOCITY\""), json);
        assertTrue(json.contains("\"payment_ids\":[\"p1\",\"p2\"]"), json);
        assertTrue(json.contains("\"detected_at_ms\":1795194005456"), json);
        assertEquals(a, AppSerdes.FRAUD_ALERT.deserializer().deserialize("fraud-alerts", bytes));
    }

    @Test
    void nullMapsToNull() {
        assertNull(AppSerdes.CARD_PROFILE.deserializer().deserialize("card-profiles", null));
        assertNull(AppSerdes.PAYMENT.serializer().serialize("payments", null));
    }

    @Test
    void malformedJsonThrowsSerializationException() {
        assertThrows(SerializationException.class,
                () -> AppSerdes.PAYMENT.deserializer().deserialize("payments", utf8("not json")));
    }
}
