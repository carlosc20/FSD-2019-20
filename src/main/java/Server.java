import Middleware.CausalOrderHandler;
import Middleware.Messages.VectorMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class Server {

    private ExecutorService c;
    private ExecutorService e;
    private ArrayList<Address> users;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;
    private Serializer s = VectorMessage.serializer;

    public Server(int id, List<Address> servers, Address address, String cluster){
        c = Executors.newFixedThreadPool(1);
        e = Executors.newFixedThreadPool(1);
        users = new ArrayList<>();
        mms = new NettyMessagingService(
            cluster,
            address,
            new MessagingConfig());
        coh = new CausalOrderHandler(id, mms, servers);
    }

    public void start(){
        mms.start();
        startListeningClients();
        startListeningCluster();
    }

    public void startListeningClients(){
        mms.registerHandler("message", (a,b) -> {
            VectorMessage m = s.decode(b);
            coh.sendToCluster(m.getMsg());
        }, c);
    }

    public void startListeningCluster(){
        Consumer<VectorMessage> cvm = (msg)-> System.out.println(msg.getMsg());
        coh.setCallback(cvm);
        mms.registerHandler("vectorMessage", (a,b)-> {
            VectorMessage m = s.decode(b);
            coh.read(m);
        }, e);
    }

    public void send(String msg) {
        coh.sendToCluster(msg);
    }
}
