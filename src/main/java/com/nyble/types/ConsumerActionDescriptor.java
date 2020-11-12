package com.nyble.types;

public class ConsumerActionDescriptor {
    private int id;
    private String description;
    private String systemId;
    private int type;

    public static final int WEB_IN = 1;
    public static final int O2O_IN = 2;
    public static final int O2O_OUT = 3;

    public ConsumerActionDescriptor(int id, String description, String systemId,  int type) {
        this.id = id;
        this.description = description;
        this.systemId = systemId;
        this.type = type;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public int getType() {
        return type;
    }
}
