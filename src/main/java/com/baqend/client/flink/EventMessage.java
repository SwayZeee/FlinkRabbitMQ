package com.baqend.client.flink;

public class EventMessage {
    public String id;
    public String name;

    public EventMessage() {
    }

    public static EventMessage fromString(String s) {
        String[] tokens = s.split(",");
        try {
            EventMessage eventmessage = new EventMessage();
            eventmessage.id = tokens[0];
            eventmessage.name = tokens[1];
            return eventmessage;
        } catch (Exception e) {
            throw e;
        }
    }
}
