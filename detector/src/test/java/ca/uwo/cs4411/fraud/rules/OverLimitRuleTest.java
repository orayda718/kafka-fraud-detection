package ca.uwo.cs4411.fraud.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Rule;

class OverLimitRuleTest {

    private final RuleTestDriver t = new RuleTestDriver(new OverLimitRule());

    @AfterEach
    void close() {
        t.close();
    }

    @Test
    void paymentAboveLimitRaisesAlert() {
        t.profile("card_1", "CA", 2000);
        t.pay("p1", "card_1", 2500, "CA", 1);
        List<FraudAlert> alerts = t.readAlerts();
        assertEquals(1, alerts.size());
        FraudAlert a = alerts.get(0);
        assertEquals(Rule.OVER_LIMIT, a.rule());
        assertEquals("card_1", a.cardId());
        assertEquals(List.of("p1"), a.paymentIds());
        assertTrue(a.reason().contains("2500.00") && a.reason().contains("2000.00"), a.reason());
    }

    @Test
    void paymentAtOrBelowLimitIsFine() {
        t.profile("card_1", "CA", 2000);
        t.pay("p1", "card_1", 1999.99, "CA", 1);
        t.pay("p2", "card_1", 2000, "CA", 2);
        assertEquals(List.of(), t.readAlerts());
    }

    @Test
    void usesEachCardsOwnLimit() {
        t.profile("card_1", "CA", 500);
        t.profile("card_2", "CA", 5000);
        t.pay("p1", "card_1", 1000, "CA", 1);
        t.pay("p2", "card_2", 1000, "CA", 2);
        List<FraudAlert> alerts = t.readAlerts();
        assertEquals(1, alerts.size());
        assertEquals("card_1", alerts.get(0).cardId());
    }

    @Test
    void profileUpdateChangesTheLimit() {
        t.profile("card_1", "CA", 2000);
        t.pay("p1", "card_1", 1500, "CA", 1);
        t.profile("card_1", "CA", 1000); // limit lowered (latest value wins on the compacted topic)
        t.pay("p2", "card_1", 1500, "CA", 2);
        List<FraudAlert> alerts = t.readAlerts();
        assertEquals(1, alerts.size());
        assertEquals(List.of("p2"), alerts.get(0).paymentIds());
    }

    @Test
    void cardWithoutProfileIsSkipped() {
        t.pay("p1", "unknown_card", 1_000_000, "CA", 1);
        assertEquals(List.of(), t.readAlerts());
    }
}
