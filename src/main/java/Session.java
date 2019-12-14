import Middleware.MessageReceive;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Session {

    CompletableFuture<List<MessageReceive>> getLast10();
    CompletableFuture<List<String>> getSubscriptions();
    void publish(String text, List<String> topics);
    void addSubscription(String name);
    void removeSubscription(String name);

}
