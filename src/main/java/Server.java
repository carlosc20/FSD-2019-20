import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Server {

    private List<Address> servers;
    private ManagedMessagingService mms;
    private Serializer s = new SerializerBuilder()
            .addType(Message.class)
            .build();

    public Server(Address address, String cluster, List<Address> servers) {
        this.servers = servers;
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

            System.out.println("Recebi preparedCheck");
        }, e);
    }


}
