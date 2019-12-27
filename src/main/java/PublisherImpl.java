import Logic.CircularArray;
import Logic.Post;
import Logic.Publisher;
import Logic.User;

import Middleware.TwoPhaseCommit.TransactionalStructures.DistributedTransactionalMap;
import Middleware.Logging.Logger;
import Middleware.Recovery;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PublisherImpl implements Publisher {

    private DistributedTransactionalMap<String, User> users;

    private HashMap<String, CircularArray<Post>> posts;

    private Recovery recovery;


    public PublisherImpl(List<String> topics, ServerMessagingService sms, Address manager, Logger log) {
        this.users = new DistributedTransactionalMap<>("users", sms, manager, log);
        this.posts = new HashMap<>();
        for(String topic: topics) {
            posts.put(topic, new CircularArray<>(10));
        }
        this.recovery = new Recovery(sms, log);
    }

    private User getAuthenticatedUser(String username, String password) {
        User user = users.get(username);
        if(user.getPassword().equals(password))
            return user;

        return null;
    }

    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        User user = users.get(username);
        return CompletableFuture.completedFuture(user.getPassword().equals(password));
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        if(users.containsKey(username))
            return CompletableFuture.completedFuture(false);

        User newUser = new User(username,password);
        users.put(username, newUser);

        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<List<Post>> getLast10(String username, String password) {
        User user = getAuthenticatedUser(username, password);
        if(user != null) {
            final int n = 10;
            List<String> subs = user.getSubscriptions();
            ArrayList<Post> a = new ArrayList<>(subs.size() * n);
            for(String sub: subs) {
                a.addAll(posts.get(sub).getAll());
            }
            // TODO sort com comparator por id
            return CompletableFuture.completedFuture(a.subList(a.size() - n, a.size()));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username, String password) {
        User user = getAuthenticatedUser(username, password);
        if(user != null) {
            return CompletableFuture.completedFuture(user.getSubscriptions());
        }
        return CompletableFuture.completedFuture(null);

    }

    @Override
    public CompletableFuture<Void> publish(String username, String password, String text, List<String> topics) {
        User user = getAuthenticatedUser(username, password);
        if(user != null) {
            // TODO calcular id
            Post post = new Post(0, username, text, topics);
            for(String topic: topics) {
                CircularArray<Post> l = posts.get(topic);
                if(l != null)
                    l.add(post);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String password, String name) {
        User user = getAuthenticatedUser(username, password);
        if(user != null) {
            user.addSubscription(name);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String password, String name) {
        User user = getAuthenticatedUser(username, password);
        if(user != null) {
            user.removeSubscription(name);
        }
        return CompletableFuture.completedFuture(null);
    }


}
