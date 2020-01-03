import Logic.Publisher;
import Middleware.GlobalSerializer;
import Middleware.Logging.Logger;
import Middleware.Logging.SubscriptionLog;
import Middleware.Logging.UnsubscriptionLog;
import Middleware.Marshalling.MessageAuth;
import Middleware.Marshalling.MessageReply;
import Middleware.Marshalling.MessageSend;
import Middleware.Marshalling.MessageSub;
import Middleware.ServerMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Server {

    private int id;
    private ServerMessagingService sms;
    private Publisher publisher;
    private Logger log;
    private Serializer s = new GlobalSerializer().build();

    private static final ArrayList<String> TOPICS =
            new ArrayList<>(Arrays.asList("Animais","Plantas","Carros"));

    public Server(int id, Address address, List<Address> servers, Address manager){
         this.id = id;
         this.log = new Logger("logs", "Server" + id, s);
         this.sms = new ServerMessagingService(id, address, servers, log, s);
         this.publisher = new PublisherImpl(TOPICS, id, manager, sms, log, (x) -> start());
    }

    private void start(){
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
            System.out.println(id + ": register request arrived");
            MessageAuth msg = s.decode(b);
            return publisher.register(msg.getUsername(), msg.getPassword())
                    .thenApply(s::encode);
        });
    }

    /*
        Recebe clientLogin e devolve boolean conforme sucesso da autenticação.
     */
    private void startListeningToLogins(){
        // client
        sms.registerCompletableOperation("clientLogin", (a,b)->{
            System.out.println(id + ": login request arrived");
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
            System.out.println(id + ": getsubs request arrived");
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth)
                    return publisher.getSubscriptions(msg.getUsername()).thenApply(subs ->
                            s.encode(new MessageReply<>(subs))
                    );
                return CompletableFuture.completedFuture(s.encode(MessageReply.ERROR(1)));
            });
        });
        sms.registerCompletableOperation("clientGetPosts", (a,b)->{
            System.out.println(id + ": getposts request arrived");
            MessageAuth msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth)
                    return publisher.getLast10(msg.getUsername()).thenApply(posts ->
                            s.encode(new MessageReply<>(posts))
                    );
                return CompletableFuture.completedFuture(s.encode(MessageReply.ERROR(1)));
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
            System.out.println(id + ": publish request arrived");
            MessageSend msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.publish(msg.getUsername(), msg.getText(), msg.getTopics()).thenApply(v -> {
                        sms.sendCausalOrderAsyncToCluster("publish", msg);
                        return s.encode(MessageReply.OK);
                    });
                }
               return CompletableFuture.completedFuture(s.encode(MessageReply.ERROR(1)));
            });
        });
        // cluster
        sms.registerOrderedOperation("publish", (a,b) ->{
            MessageSend msg = (MessageSend) b;
            publisher.publish(msg.getUsername(), msg.getText(), msg.getTopics());
        });
    }

    /*
        Recebe clientAddSub, clientRemoveSub, addSub e removeSub e atualiza o publisher
    */
    private void startListeningToSubOperations(){
        // client
        sms.registerCompletableOperation("clientAddSub", (a,b)->{
            System.out.println(id + ": addSub request arrived");
            MessageSub msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.addSubscription(msg.getUsername(), msg.getName()).thenApply(v -> {
                        log.write(new SubscriptionLog(msg));
                        sms.sendAndReceiveToCluster("addSub", msg,6);
                        return s.encode(MessageReply.OK);
                    });
                }
                return CompletableFuture.completedFuture(s.encode(MessageReply.ERROR(1)));
            });
        });
        sms.registerCompletableOperation("clientRemoveSub", (a,b)->{
            System.out.println(id + ": removeSub request arrived");
            MessageSub msg = s.decode(b);
            return publisher.login(msg.getUsername(), msg.getPassword()).thenCompose(auth -> {
                if(auth) {
                    return publisher.removeSubscription(msg.getUsername(), msg.getName()).thenApply(v -> {
                        log.write(new UnsubscriptionLog(msg));
                        sms.sendAndReceiveToCluster("removeSub", msg, 6);
                        return s.encode(MessageReply.OK);
                    });
                }
                return CompletableFuture.completedFuture(s.encode(MessageReply.ERROR(1)));
            });
        });
        // cluster
        sms.registerOperation("addSub", (a,b) ->{
            MessageSub msg = s.decode(b);
            publisher.addSubscription(msg.getUsername(), msg.getName());
            log.write(new SubscriptionLog(msg));
            return sms.encode(0);
        });
        sms.registerOperation("removeSub", (a,b) ->{
            MessageSub msg = s.decode(b);
            publisher.removeSubscription(msg.getUsername(), msg.getName());
            log.write(new UnsubscriptionLog(msg));
            return sms.encode(0);
        });
    }



    //Testes ...........................................................................................................

    public static void main(String[] args) {
        Address manager = Address.from(20000);
        ArrayList<Address> addresses = new ArrayList<>();
        int[] servers = Config.servers;
        for (int s : servers) addresses.add(Address.from(s));
        System.out.println("id:");
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        new Server(id, addresses.get(id), addresses, manager);
    }
}
