
import Middleware.MessageReceive;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Publisher {

    CompletableFuture<Boolean> login(String username, String password);
    CompletableFuture<Boolean> register(String username, String password);
    CompletableFuture<List<MessageReceive>> getLast10(String username, String password);
    CompletableFuture<List<String>> getSubscriptions(String username, String password);
    void publish(String username, String password, String text, List<String> topics);
    void addSubscription(String username, String password, String name);
    void removeSubscription(String username, String password, String name);

}
