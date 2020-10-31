package com.nyble.types;

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
}
