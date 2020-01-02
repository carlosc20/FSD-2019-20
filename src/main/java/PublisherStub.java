import Logic.Post;
import Logic.Publisher;
import Middleware.*;
import Middleware.Marshalling.*;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class PublisherStub implements Publisher {

    private ClientMessagingService ms;
    private Serializer s = new GlobalSerializer().build();
    private static final int[] servers = new int[]{10000,10001,10002};

    // definido no método login
    private String sessionPW;

    public PublisherStub(int port) {
        int rnd = new Random().nextInt(servers.length);
        ms = new ClientMessagingService(Address.from(servers[rnd]), Address.from(port));
    }

    // é preciso usar este método antes dos outros para definir as credenciais
    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientLogin").thenApply(data -> {
            boolean success = s.decode(data);
            if (success) sessionPW = password;
            return success;
        });
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientRegister").thenApply(s::decode);
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username) {
        MessageAuth msg = new MessageAuth(username, sessionPW);
        return ms.sendAndReceive(s.encode(msg),"clientGetSubs").thenApply(data -> {
            MessageReply<List<String>> reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return reply.getContent();
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<List<Post>> getLast10(String username) {
        MessageAuth msg = new MessageAuth(username, sessionPW);
        return ms.sendAndReceive(s.encode(msg),"clientGetPosts").thenApply(data -> {
            MessageReply<List<Post>> reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return reply.getContent();
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String name) {
        MessageSub msg = new MessageSub(username, sessionPW, name);
        return ms.sendAndReceive(s.encode(msg),"clientAddSub").thenApply(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return null;
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String name) {
        MessageSub msg = new MessageSub(username, sessionPW, name);
        return ms.sendAndReceive(s.encode(msg),"clientRemoveSub").thenApply(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return null;
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> publish(String username, String text, List<String> topics) {
        MessageSend msg = new MessageSend(username, sessionPW, topics, text);
        return ms.sendAndReceive(s.encode(msg),"clientPublish").thenApply(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return null;
            }
            return null;
        });
    }



}
