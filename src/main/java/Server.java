import Logic.Publisher;
import Logic.User;
import Middleware.DistributedStructures.DistributedTransactionalMap;
import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageAuth;
import Middleware.Recovery;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.Participant;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;

public class Server {
    private int id; //para debug
    private ServerMessagingService sms;
    private Publisher publisher;
    private Serializer s;
    private Recovery r;
    private DistributedTransactionalMap<String, User> users;

    public Server(int id, Address address, List<Address> servers, Address manager){
         this.id = id;
         this.s = new GlobalSerializer().getS();
         Logger log = new Logger("logs", "Server" + id, s);
         this.sms = new ServerMessagingService(id, address, servers, log);
         //this.publisher = new PublisherImpl();
         this.r = new Recovery(sms,log);
         Participant p = new Participant(sms, manager, log);
         this.users = new DistributedTransactionalMap<>("users", sms, p);
         //users.registerDistributedTransactionalPut();
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


    public void putUser(String name, User u){
        users.put(name, u);
    }

    public void localPutUser(String name, User u){
        users.localPut(name, u);
    }


    public void startListeningToText(){
        sms.registerOperation("spreadText", (a,b)->{
            System.out.println(id + ": received spread request ratatataTa");
            sms.sendCausalOrderAsyncToCluster("text", b);
        });
        sms.registerOrderedOperation("text", msg->
            System.out.println("server:startListeningToText -> received: " + msg.toString()));
        r.start(x -> {});
    }

    public void send(String text, Address a){
        MessageAuth ma = new MessageAuth("boi", "123");
        sms.send(a, ma, "spreadText");
    }

    public static void main(String[] args) throws InterruptedException {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<3; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        Thread.sleep(5000);
        Server s = new Server(id, addresses.get(id), addresses, manager);
        User u = new User("marco", "123");
        //s.startListeningToText();
        if(id == 0 || id == 1) {
            s.putUser("marco", u);
            //s.send("Olá", addresses.get(0));
        }
        //else
          //  s.localPutUser("marco", u);
    }
}
