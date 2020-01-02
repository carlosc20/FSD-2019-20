import Logic.Post;
import Logic.Publisher;
import Middleware.*;
import Middleware.Marshalling.*;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PublisherStub implements Publisher {

    private ClientMessagingService ms;
    private Serializer s = new GlobalSerializer().build();

    // definido no método login
    private String sessionPW;

    public PublisherStub(Address server) {
        final int port = 12345; // porta predefinida
        ms = new ClientMessagingService(server, Address.from(port));
    }

    // é preciso usar este método antes dos outros para definir as credenciais
    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientLogin").thenCompose(data -> {
            boolean success = s.decode(data);
            if (success) sessionPW = password;
            return CompletableFuture.completedFuture(success);
        });
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientRegister").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username) {
        MessageAuth msg = new MessageAuth(username, sessionPW);
        return ms.sendAndReceive(s.encode(msg),"clientGetSubs").thenCompose(data -> {
            MessageReply<List<String>> reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return CompletableFuture.completedFuture(reply.getContent());
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<List<Post>> getLast10(String username) {
        MessageAuth msg = new MessageAuth(username, sessionPW);
        return ms.sendAndReceive(s.encode(msg),"clientGetPosts").thenCompose(data -> {
            MessageReply<List<Post>> reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return CompletableFuture.completedFuture(reply.getContent());
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String name) {
        MessageSub msg = new MessageSub(username, sessionPW, name);
        return ms.sendAndReceive(s.encode(msg),"clientAddSub").thenCompose(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String name) {
        MessageSub msg = new MessageSub(username, sessionPW, name);
        return ms.sendAndReceive(s.encode(msg),"clientRemoveSub").thenCompose(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> publish(String username, String text, List<String> topics) {
        MessageSend msg = new MessageSend(username, sessionPW, topics, text);
        return ms.sendAndReceive(s.encode(msg),"clientPublish").thenCompose(data -> {
            MessageReply reply = s.decode(data);
            if (reply.getResponseStatusCode() == 0){
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(null);
        });
    }



}
