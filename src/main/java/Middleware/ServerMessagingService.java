package Middleware;

import Middleware.CausalOrder.CausalOrderHandler;
import Middleware.CausalOrder.VectorMessage;
import Middleware.Logging.Logger;

import Middleware.Marshalling.MessageRecovery;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServerMessagingService {
    private int id;
    CausalOrderHandler coh;
    private ExecutorService e;
    private ManagedMessagingService mms;
    private Serializer s;
    private List<Address> participants;

    public ServerMessagingService(int id, Address address, List<Address> participants, Logger log){
        this.id = id;
        this.e = Executors.newFixedThreadPool(1);
        this.mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        mms.start();
        this.s = new GlobalSerializer().s;
        this.participants = new ArrayList<>();

        int pSize = participants.size();
        //não contém ele próprio
        for(int i = 0; i<pSize; i++){
            if(i==id) continue;
            this.participants.add(participants.get(i));
        }
        this.coh = new CausalOrderHandler(id, pSize, s, log);
    }

    //public void start(){
      //  mms.start();
    //}

    public <T> void registerOperation(String type, Function<T,T> callback){
        mms.registerHandler(type, (a,b) -> {
            return s.encode(callback.apply(s.decode(b)));
        }, e);
    }

    public <T> void registerOperation(String type, Consumer<T> callback){
        mms.registerHandler(type, (a,b) -> {callback.accept(s.decode(b));}, e);
    }

    public void registerOperation(String type, BiConsumer<Address,byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOperation(String type, BiFunction<Address, byte[], byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOrderedOperation(String name, Consumer<Object> callback){
        mms.registerHandler(name, (a,b) -> {
            coh.read(0, b, o-> callback.accept(o));
        },e);
    }

    public <T> CompletableFuture<Void> sendAndReceiveToCluster(String type, T content, Function<T,T> callback){
        List<CompletableFuture<T>> requests = new ArrayList<>();
        for (Address a : participants){
            requests.add(mms.sendAndReceive(a, type, s.encode(content))
                        .thenApply(x -> callback.apply(s.decode(x))));
        }
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]));
    }


    /*
    CompletableFuture.allOf(requests.toArray(new CompletableFuture[requests.size()]))
                    .thenApply(v -> requests.stream()
                    .map(CompletableFuture::join()))
                    .collect(Collectors.toList()));
    */

    public CompletableFuture<Void> sendCausalOrderAsyncToCluster(String type, byte[] content) {
        System.out.println("sms:sendCausalOrderAsyncToCluster ->");
        Object o = s.decode(content);
        byte[] toSend = coh.createMsg(o);
        for (Address a : participants){
            mms.sendAsync(a, type, toSend);
        }
        //TODO por ao fim de tudo allOf()...talvez
        coh.logAndSaveNonAckedOperation(toSend);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAsyncToCluster(String type, Object content) {
        System.out.println("sms:sendAsyncToCluster ->");
        for (Address a : participants)
            mms.sendAsync(a, type, s.encode(content));
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAndReceiveForRecovery(String type, List<Integer> vector, Duration timout){
        //List<CompletableFuture<Void>> cfs = new ArrayList<>();
        //TODO resolver
        System.out.println("sms:sendAndReceiveForRecovery ->");
        int i = 0;
        for(Address a : participants){
            MessageRecovery mr = new MessageRecovery(id, vector.get(i));
            mms.sendAndReceive(a, type, s.encode(mr), timout, e)
                .thenAccept(b -> System.out.println("sms:sendAndReceiveForRecovery -> " + (boolean)s.decode(b) + " by " + a));
            i++;
        }
        return CompletableFuture.completedFuture(null);
    }

    public void sendOldOperation(Address address, VectorMessage msg, String type){
        System.out.println("DEBUG MESSAGE");
        System.out.println(msg.toString());
        mms.sendAsync(address, type, s.encode(msg));
    }

    public <T> CompletableFuture<T> sendAndReceive(Address a, String type, Object content){
        return mms.sendAndReceive(a, type, s.encode(content),e)
                    .thenApply(b -> s.decode(b));
    }

    public <T> CompletableFuture<Void> sendAsync(Address a, String type, T content){
        return mms.sendAsync(a,type,s.encode(content));
    }

    public <T> byte[] encode(T object){
        return s.encode(object);
    }

    public <T> T decode(byte[] bytes){
        return s.decode(bytes);
    }

    //DEBUG
    public void send(Address address, Object msg, String type){
        System.out.println("sms:send ->");
        mms.sendAsync(address, type, s.encode(msg));
    }

}
