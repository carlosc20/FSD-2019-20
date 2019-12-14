import Middleware.MessageAuth;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;

import java.util.*;


public class Server {
    private int id; //para debug
    private ServerMessagingService sms;
    private Publisher publisher;
    private Map<Address,Session> sessions;
    private Feed feed;


    public Server(int id, Address address, List<Address> servers){
         this.id = id;
         this.sms = new ServerMessagingService(id, address, servers);
         this.publisher = new PublisherImpl();
         this.sessions = new HashMap<>();
         this.feed = new Feed();
    }

    public void start(){
        sms.start();
        startListeningToLogins();
        startListeningToRegisters();
        startListeningToPublishes();
        startListeningToSubscriptions();
        startListeningToGetSubs();
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
            //User u = publisher.getUser();
            User u = null;
            if(u !=null){
                Session session = new SessionImpl(feed, u);
                sessions.put(a,session);
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
            Session session = sessions.get(a);
            return sms.encode(session.getSubscriptions());
        });
    }

    //Testes .........----------------//--------------.........
    public void startListeningToText(){
        sms.start();
        sms.registerOperation("spreadText", (a,b)->{
            System.out.println(id + ": received spread request ratatataTa");
            sms.sendAsyncToCluster("text", sms.decode(b));
        });
        //Recebe dele próprio não sei pq caralho. Se não der para resolver inserir tudo neste lado
        sms.registerOperation("text", (a,b)->{
            MessageAuth ma = sms.decode(b);
            System.out.println(id + ": received: " + ma.toString() + " from " + a.toString());
        });
    }

    public void send(String text, Address a){
        //tentei enviar só String e apareciam bytes aleatórios...passa-se com cenas primitivas ao que parece
        MessageAuth ma = new MessageAuth("boi", "123");
        sms.send(a, ma, "spreadText");
    }

    public static void main(String[] args) {
        ArrayList<Address> addresses = new ArrayList<>();
        for(int i = 0; i<4; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        Server s1 = new Server(0, addresses.get(0), addresses);
        s1.startListeningToText();
        Server s2 = new Server(1, addresses.get(1), addresses);
        s2.startListeningToText();
        Server s3 = new Server(2, addresses.get(2), addresses);
        s3.startListeningToText();
        Server s4 = new Server(3, addresses.get(3), addresses);
        s4.startListeningToText();
        s1.send("Olá", addresses.get(1));
    }
}
