package com.nyble.types;

import java.util.Objects;

public class ConsumerActionType {
    private int consumerId;
    private int systemId;
    private int actionType;

    public ConsumerActionType(int consumerId, int systemId, int actionType) {
        this.consumerId = consumerId;
        this.systemId = systemId;
        this.actionType = actionType;
    }

    public int getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(int consumerId) {
        this.consumerId = consumerId;
    }

    public int getSystemId() {
        return systemId;
    }

    public void setSystemId(int systemId) {
        this.systemId = systemId;
    }

    public int getActionType() {
        return actionType;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerActionType)) return false;
        ConsumerActionType that = (ConsumerActionType) o;
        return consumerId == that.consumerId &&
                systemId == that.systemId &&
                actionType == that.actionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerId, systemId, actionType);
    }
}
