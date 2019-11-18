import Messages.Message;
import Messages.VectorMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Client {

    private Address server;
    private ManagedMessagingService mms;
    private Serializer s = new SerializerBuilder()
            .addType(VectorMessage.class)
            .addType(Message.class)
            .addType(Vector.class)
            .build();

    public Client(Address address, String cluster, Address server) {
        this.server = server;
        this.mms = new NettyMessagingService(
                cluster,
                address,
                new MessagingConfig());
    }

    void start() {

        mms.start();

        ScheduledExecutorService e = Executors.newScheduledThreadPool(1);

        mms.registerHandler("coiso", (a,b)-> {
            Message m = s.decode(b);

        }, e);
    }

    void send(Message m) {
        System.out.println("Enviei para " + server.port());
        mms.sendAsync(server, "line", s.encode(m));
    }

}
