package house.greysap.model;

import java.sql.Timestamp;

public class CustomerTransaction {
    private long transactionId;
    private Timestamp eventTime;
    private int customerId;
    private String transactionType;
    private double amount;

    public CustomerTransaction(long transactionId, Timestamp eventTime, int customerId, String transactionType, double amount) {
        this.transactionId = transactionId;
        this.eventTime = eventTime;
        this.customerId = customerId;
        this.transactionType = transactionType;
        this.amount = amount;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public Timestamp getEventTime() {
        return eventTime;
    }

    public void setEventTime(Timestamp eventTime) {
        this.eventTime = eventTime;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }
}