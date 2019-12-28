import Logic.CircularArray;
import Logic.Post;
import Logic.Publisher;
import Logic.User;

import Middleware.Marshalling.MessageAuth;
import Middleware.TwoPhaseCommit.DistributedObjects.TransactionalMap;
import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class PublisherImpl implements Publisher {

    private TransactionalMap<String, User> users;

    private ServerMessagingService sms;
    private HashMap<String, CircularArray<Post>> posts;

    public PublisherImpl(List<String> topics, ServerMessagingService sms, Address manager, Logger log) {
        this.sms = sms;
        this.users = new TransactionalMap<>(sms, manager, log);
        this.posts = new HashMap<>();
        for(String topic: topics) {
            posts.put(topic, new CircularArray<>(10));
        }
        sms.causalOrderRecover(recoverOperations, log);
    }

    private void startListeningToNeighboursPublishes(){
        sms.registerOrderedOperation("publish", msg -> {
            //TODO
        });
    }

    private void startListeningToNeighboursSubOperations(){
        sms.registerOperation("addSub", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            // TODO
        });

        sms.registerOperation("removeSub", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            // TODO
        });

    }

    private Consumer<Object> recoverOperations = (content) -> {
        //TODO
    };


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
    public CompletableFuture<List<Post>> getLast10(String username) {
        final int n = 10;
        User user = users.get(username);
        List<String> subs = user.getSubscriptions();
        ArrayList<Post> a = new ArrayList<>(subs.size() * n);
        for(String sub: subs) {
            a.addAll(posts.get(sub).getAll());
        }
        // TODO sort com comparator por id
        return CompletableFuture.completedFuture(a.subList(a.size() - n, a.size()));

    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username) {
        User user = users.get(username);
        return CompletableFuture.completedFuture(user.getSubscriptions());
    }

    @Override
    public CompletableFuture<Void> publish(String username, String text, List<String> topics) {
        // TODO calcular id
        Post post = new Post(0, username, text, topics);
        for(String topic: topics) {
            CircularArray<Post> l = posts.get(topic);
            if(l != null)
                l.add(post);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String name) {
        User user = users.get(username);
        user.addSubscription(name);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String name) {
        User user = users.get(username);
        user.removeSubscription(name);
        return CompletableFuture.completedFuture(null);
    }


}
