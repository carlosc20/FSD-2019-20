import Logic.Publisher;
import Logic.User;
import Middleware.GlobalSerializer;
import Middleware.Marshalling.MessageAuth;
import Middleware.Recovery;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;

public class Server {
    private int id; //para debug
    private ServerMessagingService sms;
    private Publisher publisher;
    private Serializer s;
    private Recovery r;

    public Server(int id, Address address, List<Address> servers){
         this.id = id;
         this.sms = new ServerMessagingService(id, address, servers);
         //this.publisher = new PublisherImpl();
         this.s = new GlobalSerializer().getS();
         this.r = new Recovery(sms);
         //this.loggers = new HashMap<>();
         //this.loggers.put("Users", new Logger("Users", s));
         //this.loggers.put("Publishes", new Logger("Publishes", s));
    }

    public void start(){
       // sms.start();
        startListeningToLogins();
        startListeningToRegisters();
        startListeningToPublishes();
        startListeningToSubscriptions();
        startListeningToGetSubs();
        //TODO
        r.start(x -> {});
    }

    //Receção de tópicos
    private void startListeningToPublishes(){
        //Pode haver return nos handlers se necessário
        //Envios de clientes
        sms.registerOperation("clientPublish", (a,b)->{
               //TODO mete em algum lado neste servidor
                sms.sendCausalOrderAsyncToCluster("publish", b);
        });
        //Envios de servidores
        sms.registerOrderedOperation("publish", msg -> {
            //TODO mete em algum lado neste servidor
        });
    }

    private void startListeningToRegisters(){
        sms.registerOperation("clientRegister", (a,b)->{
            MessageAuth msg = sms.decode(b);
            //TODO meter algures o registo
            //O que envias aqui é duvidoso. CompletableFuture ?
            sms.sendAsyncToCluster("register", sms.encode(publisher.register(msg.getUsername(),msg.getPassword())));
            return sms.encode(true); // resultado do registar
        });
        sms.registerOperation("register", (a,b) ->{
            //TODO inserir o registo
        });
    }

    private void startListeningToLogins(){
        sms.registerOperation("clientLogin", (a,b)->{
            MessageAuth msg = sms.decode(b);
            //TODO
            //Logic.User u = publisher.getUser();
            User u = null;
            if(u !=null){
                //Session session = new SessionImpl(feed, u);
                //sessions.put(a,session);
                sms.sendAsyncToCluster("login", sms.encode(publisher.login(msg.getUsername(), msg.getPassword())));
                return sms.encode(true);
            }
        return sms.encode(false);
        });
    }

    private void startListeningToSubscriptions(){
        sms.registerOperation("clientSubscription", (a,b)->{
            //TODO subscrição
            sms.sendAsyncToCluster("subscription", b);
        });
        sms.registerOperation("subscription", (a,b) ->{
            //TODO addSubscription
        });
    }

    private void startListeningToGetSubs(){
        sms.registerOperation("getSubs", (a,b)->{
            //Session session = sessions.get(a);
            //return sms.encode(session.getSubscriptions());
        });
    }

    //Testes .........----------------//--------------.........
    public void startListeningToText(){
        sms.registerOperation("spreadText", (a,b)->{
            System.out.println(id + ": received spread request ratatataTa");
            sms.sendCausalOrderAsyncToCluster("text", b);
        });
        sms.registerOrderedOperation("text", msg->
            System.out.println(id + ": received: " + msg.toString() + " from " + msg.toString()));
        r.start(x -> {});
    }

    public void send(String text, Address a){
        MessageAuth ma = new MessageAuth("boi", "123");
        sms.send(a, ma, "spreadText");
        System.out.println("Client sending hello");
    }

    public static void main(String[] args) throws InterruptedException {
        ArrayList<Address> addresses = new ArrayList<>();
        for(int i = 0; i<4; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        Thread.sleep(10000);
        Server s1 = new Server(id, addresses.get(id), addresses);
        s1.startListeningToText();
        if(id == 0){
            s1.send("Olá", addresses.get(1));
            Thread.sleep(10000);}
    }
}
