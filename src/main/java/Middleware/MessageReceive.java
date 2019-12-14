package Middleware;

import java.util.List;

public class MessageReceive {

    private String sender;
    private List<String> topics;
    private String text;

    public MessageReceive(String sender, List<String> topics, String text) {
        this.sender = sender;
        this.topics = topics;
        this.text = text;
    }

    public String getSender() {
        return sender;
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getText() {
        return text;
    }

    public String toSring() {
        return sender + ": " + text;
    }
}
