import Middleware.MessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SessionStub implements Session{

    private MessagingService ms;
    private Serializer s = new SerializerBuilder()
            .addType(ArrayList.class)
            .addType(String.class)
            .build();

    public SessionStub(Address server) {
        final int port = 12345;
        ms = new MessagingService(server, Address.from(port));
        ms.start();
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptions() {
        return ms.sendAndReceive(null,"getSubs").thenCompose(s::decode);
    }

    @Override
    public void addSubscription(String name) {
        ms.send(s.encode(name),"addSub");
    }

    @Override
    public void removeSubscription(String name) {
        ms.send(s.encode(name),"removeSub");
    }

    @Override
    public void publish(String text, List<String> topics) {
        MessageSend message = new MessageSend(topics, text);
        ms.send(s.encode(message),"publish");
    }

    @Override
    public CompletableFuture<List<MessageReceive>> getLast10() {
        return ms.sendAndReceive(null,"get10").thenCompose(s::decode);
    }
}
