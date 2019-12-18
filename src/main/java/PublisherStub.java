import Logic.Publisher;
import Middleware.*;
import Middleware.Marshalling.MessageAuth;
import Middleware.Marshalling.MessageReceive;
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
        ms.start();
    }

    @Override
    public CompletableFuture<Boolean> login(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"ClientLogin").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"clientRegister").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"getSubs").thenCompose(s::decode);
    }

    @Override
    public void addSubscription(String username, String password, String name) {
        MessageSub msg = new MessageSub(username, password, name);
        ms.send(s.encode(msg),"addSub");
    }

    @Override
    public void removeSubscription(String username, String password, String name) {
        MessageSub msg = new MessageSub(username, password, name);
        ms.send(s.encode(msg),"removeSub");
    }

    @Override
    public void publish(String username, String password, String text, List<String> topics) {
        MessageSend message = new MessageSend(username, password, topics, text);
        ms.send(s.encode(message),"clientPublish");
    }

    @Override
    public CompletableFuture<List<MessageReceive>> getLast10(String username, String password) {
        MessageAuth msg = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(msg),"get10").thenCompose(s::decode);
    }

}
