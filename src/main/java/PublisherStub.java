import Logic.Post;
import Logic.Publisher;
import Middleware.*;
import Middleware.Marshalling.MessageAuth;
import Middleware.Marshalling.MessageSend;
import Middleware.Marshalling.MessageSub;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PublisherStub implements Publisher {

    private ClientMessagingService ms;
    private Serializer s = new SerializerBuilder()
        .addType(ArrayList.class)
        .addType(String.class)
        .build();

    public PublisherStub(Address server) {
        final int port = 12345;
        ms = new ClientMessagingService(server, Address.from(port));
    }

    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientLogin").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientRegister").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientGetSubs").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Void> addSubscription(String username, String password, String name) {
        MessageSub msg = new MessageSub(username, password, name);
        return ms.sendAndReceive(s.encode(msg),"clientAddSub").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Void> removeSubscription(String username, String password, String name) {
        MessageSub msg = new MessageSub(username, password, name);
        return ms.sendAndReceive(s.encode(msg),"clientRemoveSub").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Void> publish(String username, String password, String text, List<String> topics) {
        MessageSend msg = new MessageSend(username, password, topics, text);
        return ms.sendAndReceive(s.encode(msg),"clientPublish").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<List<Post>> getLast10(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientGetPosts").thenCompose(s::decode);
    }

}
