import java.util.List;

public class MessageReceive {

    private String sender;
    private List<String> topics;
    private String text;


    public String toSring() {
        return sender + ": " + text;
    }
}
