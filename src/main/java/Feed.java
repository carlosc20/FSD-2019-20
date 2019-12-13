import java.util.ArrayList;
import java.util.List;

public class Feed {

    private List<MessageReceive> messages;

    public Feed() {
        this.messages = new ArrayList<>();
    }

    public List<MessageReceive> getLastMessages(int n, List<String> topics) {
        // TODO filtrar por topicos
        int s = messages.size();
        return messages.subList(s - n,s);
    }

    public void publish(MessageReceive message) {
        messages.add(message);
    }

}
