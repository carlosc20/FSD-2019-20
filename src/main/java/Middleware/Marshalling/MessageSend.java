package Middleware.Marshalling;

import java.util.List;

public class MessageSend extends MessageAuth {

    private List<String> topics;
    private String text;

    public MessageSend(String username, String password, List<String> topics, String text) {
        super(username, password);
        this.topics = topics;
        this.text = text;
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "MessageSend{" +
                "topics=" + topics +
                ", text='" + text + '\'' +
                '}';
    }
}
