import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Publisher {

    CompletableFuture<Boolean> login(String username, String password);
    CompletableFuture<Boolean> register(String username, String password);
    CompletableFuture<List<String>> getSubscriptions();
    void addSubscription(String name);
    void removeSubscription(String name);
    void publish(String text, List<String> topics);
    CompletableFuture<List<MessageReceive>> getLast10();
}
