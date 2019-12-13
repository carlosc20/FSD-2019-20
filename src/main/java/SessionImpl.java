import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SessionImpl implements Session {

    private Feed feed;
    private User user;

    public SessionImpl(Feed feed, User user) {
        this.feed = feed;
        this.user = user;
    }

    @Override
    public CompletableFuture<List<MessageReceive>> getLast10() {
        return CompletableFuture.completedFuture(feed.getLastMessages(10, user.getSubscriptions()));
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions() {
        return CompletableFuture.completedFuture(user.getSubscriptions());
    }

    @Override
    public void publish(String text, List<String> topics) {
        MessageReceive msg = new MessageReceive(user.getName(), topics, text);
        feed.publish(msg);
    }

    @Override
    public void addSubscription(String name) {
        user.addSubscription(name);
    }

    @Override
    public void removeSubscription(String name) {
        user.removeSubscription(name);
    }
}
