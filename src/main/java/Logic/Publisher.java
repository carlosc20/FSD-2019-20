package Logic;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Publisher {

    CompletableFuture<Boolean> login(String username, String password);
    CompletableFuture<Boolean> register(String username, String password);
    CompletableFuture<List<Post>> getLast10(String username);
    CompletableFuture<List<String>> getSubscriptions(String username);
    CompletableFuture<Void> publish(String username, String text, List<String> topics);
    CompletableFuture<Void> addSubscription(String username, String name);
    CompletableFuture<Void> removeSubscription(String username, String name);

}
