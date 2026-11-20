package ca.uwo.cs4411.fraud.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Rule;

/** Default thresholds: more than 5 payments within 60 s, 10 s grace. */
class VelocityRuleTest {

    private final RuleTestDriver t = new RuleTestDriver(new VelocityRule());

    @AfterEach
    void close() {
        t.close();
    }

    /** Sends n payments on one card, starting at startSec, gapSec apart. Ids are prefix0, prefix1, ... */
    private void burst(String card, String prefix, int n, double startSec, double gapSec) {
        for (int i = 0; i < n; i++) {
            t.pay(prefix + i, card, 10, "CA", startSec + i * gapSec);
        }
    }

    @Test
    void fivePaymentsInAMinuteIsFine() {
        burst("card_1", "p", 5, 0, 2);
        assertEquals(List.of(), t.readAlerts());
    }

    @Test
    void sixPaymentsInAMinuteRaisesOneAlert() {
        burst("card_1", "p", 6, 0, 2);
        List<FraudAlert> alerts = t.readAlerts();
        assertEquals(1, alerts.size());
        FraudAlert a = alerts.get(0);
        assertEquals("card_1", a.cardId());
        assertEquals(Rule.VELOCITY, a.rule());
        assertEquals(List.of("p0", "p1", "p2", "p3", "p4", "p5"), a.paymentIds());
        assertTrue(a.reason().contains("6 transactions"), a.reason());
    }

    @Test
    void longerBurstStillRaisesExactlyOneAlert() {
        burst("card_1", "p", 12, 0, 1);
        assertEquals(1, t.readAlerts().size());
    }

    @Test
    void burstAcrossAMinuteBoundaryIsCaught() {
        // 8 payments from 55 s to 69 s: a fixed 0-60 / 60-120 window would split them 3 + 5.
        burst("card_1", "p", 8, 55, 2);
        assertEquals(1, t.readAlerts().size());
    }

    @Test
    void spreadOutPaymentsDoNotAlert() {
        // One every 15 s: any 60 s span holds at most 5.
        burst("card_1", "p", 10, 0, 15);
        assertEquals(List.of(), t.readAlerts());
    }

    @Test
    void separateBurstsRaiseSeparateAlerts() {
        burst("card_1", "a", 6, 0, 1);
        burst("card_1", "b", 6, 300, 1);
        List<FraudAlert> alerts = t.readAlerts();
        assertEquals(2, alerts.size());
        assertTrue(alerts.get(0).paymentIds().contains("a0"));
        assertTrue(alerts.get(1).paymentIds().contains("b0"));
    }

    @Test
    void cardsAreCountedSeparately() {
        for (int i = 0; i < 4; i++) {
            t.pay("x" + i, "card_1", 10, "CA", i);
            t.pay("y" + i, "card_2", 10, "CA", i);
        }
        assertEquals(List.of(), t.readAlerts());
    }

    @Test
    void latePaymentsRespectGracePeriod() {
        burst("card_1", "p", 5, 0, 1);          // 0..4 s
        t.pay("other", "card_2", 10, "CA", 200); // stream time moves to 200 s
        // Every 60 s window containing 5 s closed by 75 s (end + 10 s grace): dropped.
        t.pay("late", "card_1", 10, "CA", 5);
        assertEquals(List.of(), t.readAlerts());

        burst("card_3", "q", 5, 300, 1);         // 300..304 s
        t.pay("late3", "card_3", 10, "CA", 298); // only 6 s behind stream time: counted
        assertEquals(1, t.readAlerts().size());
    }
}
