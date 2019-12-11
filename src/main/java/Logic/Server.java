package Logic;

public class Server {
/*
    private ExecutorService c;
    private ExecutorService e;
    private ArrayList<Address> users;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;
    //TODO meter um serializer
    private GlobalSerializer s;

    public Logic.Server(int id, List<Address> servers, Address address, String cluster){
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
            //coh.sendToCluster(m.getMsg());
        }, c);
    }

    public void startListeningCluster(){

        Consumer<VectorMessage> cvm = (msg)-> System.out.println(msg.getMsg());
        mms.registerHandler("vectorMessage", (a,b)-> {
            VectorMessage m = s.decode(b);
            coh.read(m, cvm);
        }, e);

    }

    public void send(String msg) {
        coh.sendToCluster(msg);
    }
      */
}
