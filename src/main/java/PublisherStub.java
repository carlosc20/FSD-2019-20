import Middleware.ClientMessagingService;
import Middleware.MessageAuth;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
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
        MessageAuth message = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(message),"ClientLogin").thenCompose(s::decode);
    }

    @Override
    public CompletableFuture<Boolean> register(String username, String password) {
        MessageAuth message = new MessageAuth(username, password);
        return ms.sendAndReceive(s.encode(message),"clientRegister").thenCompose(s::decode);
    }



}
