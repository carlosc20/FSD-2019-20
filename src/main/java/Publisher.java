import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Publisher {

    boolean login(String username, String password);
    void register(String username, String password);
    CompletableFuture<List<String>> getSubscriptions();
    void addSubscription(String name);
    void removeSubscription(String name);
    void publish(String text, List<String> topics);
    CompletableFuture<List<MessageReceive>> getLast10();
}
