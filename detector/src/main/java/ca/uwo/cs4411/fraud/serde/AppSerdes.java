package ca.uwo.cs4411.fraud.serde;

import ca.uwo.cs4411.fraud.model.CardProfile;
import ca.uwo.cs4411.fraud.model.FraudAlert;
import ca.uwo.cs4411.fraud.model.Payment;

/** Shared serde instances for the project's topics. */
public final class AppSerdes {

    private AppSerdes() {
    }

    public static final JsonSerde<Payment> PAYMENT = new JsonSerde<>(Payment.class);
    public static final JsonSerde<CardProfile> CARD_PROFILE = new JsonSerde<>(CardProfile.class);
    public static final JsonSerde<FraudAlert> FRAUD_ALERT = new JsonSerde<>(FraudAlert.class);
}
