import Logic.Publisher;
import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageAuth;
import Middleware.Marshalling.MessageSend;
import Middleware.Marshalling.MessageSub;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;

public class Server {

    private int id; //para debug
    private ServerMessagingService sms;
    private Publisher publisher;
    private Serializer s;

    public Server(int id, Address address, List<Address> servers, Address manager){
         this.id = id;
         Serializer s = new GlobalSerializer().build();
         Logger log = new Logger("logs", "Server" + id, s);
         this.sms = new ServerMessagingService(id, address, servers, log, s);
         List<String> topics = new ArrayList<>();
         this.publisher = new PublisherImpl(topics, sms, manager, log);
    }

    public void start(){
        startListeningToLogins();
        startListeningToRegisters();
        startListeningToPublishes();
        startListeningToGets();
        startListeningToSubOperations();
    }

    /*
        Recebe clientRegister e devolve boolean conforme sucesso do registo e atualiza publisher.
        Recebe register e atualiza publisher.
     */
    private void startListeningToRegisters(){
        // client
        sms.registerCompletableOperation("clientRegister", (a,b)->{
            MessageAuth msg = sms.decode(b);
            return publisher.register(msg.getUsername(), msg.getPassword()).thenApply(success -> {
                if(success) {
                    sms.sendAsyncToCluster("register", b);
                    return s.encode(true);
                }
                else
                    return s.encode(false);
            });
        });
        // cluster
        sms.registerOperation("register", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            publisher.register(msg.getUsername(), msg.getPassword());
        });
    }

    /*
        Recebe clientLogin e devolve boolean conforme sucesso da autenticação.
     */
    private void startListeningToLogins(){
        // client
        sms.registerCompletableOperation("clientLogin", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenApply(s::encode);
        });
    }


    /*
        Recebe clientGetSubs e clientGetPosts, devolve a informação pedida
     */
    private void startListeningToGets(){
        // client
        sms.registerCompletableOperation("clientGetSubs", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth)
                    return publisher.getSubscriptions(msg.getUsername()).thenApply(s::encode);
                return null; // TODO mensagem erro
            });
        });
        sms.registerCompletableOperation("clientGetPosts", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth)
                    return publisher.getLast10(msg.getUsername()).thenApply(s::encode);
                return null; // TODO mensagem erro
            });
        });
    }


    /*
    Recebe clientPublish, atualiza publisher e devolve FEEDBACK
    Recebe publish, atualiza publisher
    */
    private void startListeningToPublishes(){
        // client
        sms.registerCompletableOperation("clientPublish", (a,b)->{
            MessageSend msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.publish(msg.getUsername(), msg.getText(), msg.getTopics()).thenApply(nada -> {
                        //TODO criar msg para cluster sem passwd?
                        sms.sendCausalOrderAsyncToCluster("publish", b);
                        return s.encode(null); // TODO ???
                    });
                }
                return null; // TODO mensagem erro
            });
        });
        // cluster
        sms.registerOperation("publish", (a,b) ->{
            MessageSend msg = sms.decode(b); //TODO criar msg para cluster sem passwd?
            publisher.publish(msg.getUsername(), msg.getText(), msg.getTopics());
        });
    }

    /*
        Recebe clientAddSub, clientRemoveSub, addSub e removeSub e atualiza o publisher
    */
    private void startListeningToSubOperations(){
        // client
        sms.registerCompletableOperation("clientAddSub", (a,b)->{
            MessageSub msg = sms.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.addSubscription(msg.getUsername(), msg.getName()).thenApply(nada -> {
                        //TODO criar msg para cluster sem passwd?
                        sms.sendAsyncToCluster("addSub", b);
                        return s.encode(null); // ????
                    });
                }
                return null; // TODO mensagem erro
            });
        });
        sms.registerCompletableOperation("clientRemoveSub", (a,b)->{
            MessageSub msg = sms.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.removeSubscription(msg.getUsername(), msg.getName()).thenApply(nada -> {
                        //TODO criar msg para cluster sem passwd?
                        sms.sendAsyncToCluster("removeSub", b);
                        return s.encode(null); // ????
                    });
                }
                return null; // TODO mensagem erro
            });
        });
        // cluster
        sms.registerOperation("addSub", (a,b) ->{
            MessageSub msg = sms.decode(b); //TODO criar msg para cluster sem passwd?
            publisher.addSubscription(msg.getUsername(), msg.getName());
        });
        sms.registerOperation("removeSub", (a,b) ->{
            MessageSub msg = sms.decode(b); //TODO criar msg para cluster sem passwd?
            publisher.removeSubscription(msg.getUsername(), msg.getName());
        });
    }



    //Testes ..............................................................................................

    /*
        public void putUser(String name, User u){
            users.put(name, u);
        }

        public void localPutUser(String name, User u){
            users.localPut(name, u);
        }
    */

    private void startListeningToLogins2(){
        sms.registerCompletableOperation("clientLogin", (a,b)->{
            MessageAuth msg = sms.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenApply(success -> {
                if(success) {
                    return sms.encode(true);
                }
                else
                    return sms.encode(false);
            });
        });
    }

    public void startListeningToText(){
        sms.registerOperation("spreadText", (a,b)->{
            System.out.println(id + ": received spread request ratatataTa");
            sms.sendCausalOrderAsyncToCluster("text", b);
        });
        sms.registerOrderedOperation("text", msg->
            System.out.println("server:startListeningToText -> received: " + msg.toString()));
    }

    public void send(String text, Address a){
        MessageAuth ma = new MessageAuth("boi", "123");
        sms.send(a, ma, "spreadText");
    }

    public static void main(String[] args) throws InterruptedException {
        ArrayList<Address> addresses = new ArrayList<>();
        Address manager = Address.from("localhost", 20000);
        for(int i = 0; i<2; i++){
            addresses.add(Address.from("localhost",10000 + i));
        }
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        Thread.sleep(5000);
        Server s = new Server(id, addresses.get(id), addresses, manager);
        s.startListeningToText();
        if(id == 0 ) {
            s.publisher.register("marco", "123");
        }
            //s.send("Olá", addresses.get(0));
        //}
    }
}
