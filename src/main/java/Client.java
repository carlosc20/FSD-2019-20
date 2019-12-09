
import Middleware.CausalOrdering.VectorMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Client {

    private Address server;
    private ManagedMessagingService mms;
    //TODO meter um serializer
    private Serializer s;

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
            VectorMessage m = s.decode(b);
            System.out.println("Recebi: " + m);
        }, e);
    }

    void send(String msg) {
        //VectorMessage m = new VectorMessage(mms.address().port(),msg);
        //System.out.println("Enviei para " + server.port() + ": " + m);
        //mms.sendAsync(server, "message", s.encode(m));
    }

}
