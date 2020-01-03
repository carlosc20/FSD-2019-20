import Logic.CircularArray;
import Logic.Post;
import Logic.Publisher;
import Logic.User;

import Middleware.Marshalling.MessageSend;
import Middleware.Recovery.Recovery;
import Middleware.TwoPhaseCommit.DistributedObjects.TransactionalMap;
import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.Participant;
import io.atomix.utils.net.Address;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class PublisherImpl implements Publisher {

    private TransactionalMap<String, User> users;
    private int lastPostId;
    private HashMap<String, CircularArray<Post>> posts;
    private static final int n = 10; // nr de posts devolvidos no getLast


    public PublisherImpl(List<String> topics, int id, Address manager, ServerMessagingService sms, Logger log, Consumer<Integer> serverStart) {
        this.lastPostId = 0;
        Participant p = new Participant(id, manager, sms, log);
        this.users = new TransactionalMap<>(p);
        this.posts = new HashMap<>();
        for(String topic: topics) {
            posts.put(topic, new CircularArray<>(n));
        }
        new Recovery(log,sms).start((obj) -> {
            //TODO colocar as que faltam
            MessageSend msg = (MessageSend) obj;
            publish(msg.getUsername(), msg.getText(), msg.getTopics());
        }, users, serverStart);
        users.start();
    }

    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        User user = users.get(username);
        if(user == null) return CompletableFuture.completedFuture(false);
        return CompletableFuture.completedFuture(user.getPassword().equals(password));
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        if(users.containsKey(username))
            return CompletableFuture.completedFuture(false);

        User newUser = new User(username,password);
        return users.put(username, newUser);
    }

    @Override
    public CompletableFuture<List<Post>> getLast10(String username) {

        User user = users.get(username);
        Set<String> subs = user.getSubscriptions();
        ArrayList<Post> posts = new ArrayList<>(subs.size() * n);
        for(String sub: subs) {
            posts.addAll(this.posts.get(sub).getAll());
        }
        posts.sort(Comparator.comparing(Post::getId));
        return CompletableFuture.completedFuture(posts.subList(posts.size() - n, posts.size()));

    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username) {
        User user = users.get(username);
        return CompletableFuture.completedFuture(new ArrayList<>(user.getSubscriptions()));
    }

    @Override
    public CompletableFuture<Void> publish(String username, String text, List<String> topics) {
        Post post = new Post(lastPostId, username, text, topics);
        lastPostId++;
        for(String topic: topics) {
            CircularArray<Post> l = posts.get(topic);
            if(l != null)
                l.add(post);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String name) {
        if (posts.containsKey(name)) {
            User user = users.get(username);
            user.addSubscription(name);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String name) {
        User user = users.get(username);
        user.removeSubscription(name);
        return CompletableFuture.completedFuture(null);
    }


}
