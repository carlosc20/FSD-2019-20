import java.util.List;

public class MessageSend {

    private List<String> topics;
    private String text;

    public MessageSend(List<String> topics, String text) {
        this.topics = topics;
        this.text = text;
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getText() {
        return text;
    }
}
