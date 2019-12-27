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
    private Serializer s = new GlobalSerializer().getS();

    public Server(int id, Address address, List<Address> servers, Address manager){
         this.id = id;
         Serializer s = new GlobalSerializer().getS();
         Logger log = new Logger("logs", "Server" + id, s);
         this.sms = new ServerMessagingService(id, address, servers, log);
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
        sms.registerOperation("register", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            publisher.register(msg.getUsername(), msg.getPassword());
        });
    }

    /*
        Recebe clientLogin e devolve boolean conforme sucesso da autenticação.
     */
    private void startListeningToLogins(){
        sms.registerCompletableOperation("clientLogin", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenApply(s::encode);
        });
    }


    /*
        Recebe clientPublish, atualiza publisher e devolve FEEDBACK
        Recebe publish, atualiza publisher
     */
    private void startListeningToPublishes(){
        sms.registerCompletableOperation("clientPublish", (a,b)->{
            MessageSend msg = s.decode(b);
            return publisher.publish(msg.getUsername(), msg.getPassword(), msg.getText(), msg.getTopics()).thenApply(nada -> {
                //TODO
                sms.sendCausalOrderAsyncToCluster("publish", b);
                return null;
            });
        });
        sms.registerOrderedOperation("publish", msg -> {
            //TODO
        });
    }

    /*
        Recebe clientGetSubs e clientGetPosts, devolve o pedido
     */
    private void startListeningToGets(){
        sms.registerCompletableOperation("clientGetSubs", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.getSubscriptions(msg.getUsername(), msg.getPassword()).thenApply(s::encode);
        });
        sms.registerCompletableOperation("clientGetPosts", (a,b)->{
            MessageAuth msg = s.decode(b);
            return publisher.getLast10(msg.getUsername(), msg.getPassword()).thenApply(s::encode);
        });
    }


    /*
        Recebe clientAddSub, clientRemoveSub, addSub e removeSub e atualiza o publisher
    */
    private void startListeningToSubOperations(){
        sms.registerCompletableOperation("clientAddSub", (a,b)->{
            MessageSub msg = sms.decode(b);
            return publisher.addSubscription(msg.getUsername(), msg.getPassword(), msg.getName()).thenApply(nada -> {
                // TODO
                sms.sendAsyncToCluster("addSub", b);
                return null; // ????
            });
        });
        sms.registerOperation("addSub", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            // TODO
        });

        sms.registerCompletableOperation("clientRemoveSub", (a,b)->{
            MessageSub msg = sms.decode(b);
            return publisher.removeSubscription(msg.getUsername(), msg.getPassword(), msg.getName()).thenApply(success -> {
                // TODO
                sms.sendAsyncToCluster("removeSub", b);
                return null; // ????
            });
        });
        sms.registerOperation("removeSub", (a,b) ->{
            MessageAuth msg = sms.decode(b);
            // TODO
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
        s.publisher.register("marco", "123");
        //s.startListeningToText();
        //if(id == 0 || id == 1) {
            //s.putUser("marco", u);
            //s.send("Olá", addresses.get(0));
        //}
        //else
          //  s.localPutUser("marco", u);
    }
}
