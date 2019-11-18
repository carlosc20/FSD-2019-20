import Messages.VectorMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class Server {
    private int id;
    private ExecutorService c;
    private ExecutorService e;
    private ArrayList<Address> users;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;

    public Server(int id, int numParticipants, ArrayList<Address> servers, ManagedMessagingService mms){
        this.id = id;
        c = Executors.newFixedThreadPool(1);
        e = Executors.newFixedThreadPool(1);
        users = new ArrayList<>();
        coh = new CausalOrderHandler(id, numParticipants, mms, servers);
        this.mms = mms;
    }

    public void start(){
        mms.start();
        startListeningClients(mms);
        startListeningCluster(mms);
    }

    public void startListeningClients(ManagedMessagingService ms){
        ms.registerHandler("message", (c,b) -> {
            String m = new String(b);
            coh.sendToCluster(m);
        }, c);
    }

    public void startListeningCluster(ManagedMessagingService ms){
        Consumer<VectorMessage> cvm = (msg)-> System.out.println(msg.getMsg());
        coh.setCallback(cvm);
        ms.registerHandler("vectorMessage", (c,b)-> {
            VectorMessage m = new VectorMessage();
            m.fillFromByteArray(b);
            coh.read(m);
        }, e);
    }
/*
    public void send(String msg) {
        for (ManagedMessagingService ms : mss) {
            VectorMessage m = new VectorMessage();
            for (Address a : servers)
                ms.sendAsync(a, "vectorMessage", encode(m));
        }
    }
*/
}
