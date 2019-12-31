package Middleware;

import Middleware.CausalOrder.CausalOrderHandler;
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
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class ServerMessagingService {
    private int id;
    private CausalOrderHandler coh;
    private ScheduledExecutorService ses;
    private ExecutorService e;
    private ManagedMessagingService mms;
    private Serializer s;
    private List<Address> participants;

    public ServerMessagingService(int id, Address address, List<Address> participants, Logger log, Serializer s){
        this.id = id;
        //TODO passar executor para fora?
        this.ses = Executors.newScheduledThreadPool(1);
        this.e = Executors.newFixedThreadPool(1);
        this.mms = new NettyMessagingService(
                "server",
                address,
                new MessagingConfig());
        this.participants = new ArrayList<>();
        this.s = s;
        int pSize = participants.size();
        //não contém ele próprio
        for(int i = 0; i<pSize; i++){
            if(i==id) continue;
            this.participants.add(participants.get(i));
        }
        this.coh = new CausalOrderHandler(id, pSize, s, log);
    }

    public void start(){
        mms.start();
        registerOperation("causalOrderRecovery", (a,b)->{
            System.out.println("recovery:handler -> Received request from: " + a);
            boolean state = coh.treatRecoveryRequest(decode(b),
                    msg2 -> sendAsync(a, msg2.getOperation(), msg2));
            return encode(state);
        });
    }

    public <T> void registerOperation(String type, Consumer<T> callback){
        mms.registerHandler(type, (a,b) -> {callback.accept(s.decode(b));}, e);
    }

    public void registerCompletableOperation(String type, BiFunction<Address, byte[], CompletableFuture<byte[]>> callback){
        mms.registerHandler(type, callback);

    }

    public void registerOperation(String type, BiConsumer<Address,byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOperation(String type, BiFunction<Address, byte[], byte[]> callback){
        mms.registerHandler(type, callback, e);
    }

    public void registerOrderedOperation(String name, Consumer<Object> callback){
        mms.registerHandler(name, (a,b) -> {
            coh.read(b, callback);
        },e);
    }

    public <T> CompletableFuture<Void> sendAndReceiveToCluster(String type, T content, Consumer<T> callback){
        List<CompletableFuture<Void>> requests = new ArrayList<>();
        for (Address a : participants){
            requests.add(mms.sendAndReceive(a, type, s.encode(content))
                        .thenAccept(x -> callback.accept(s.decode(x))));
        }
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]));
    }

    public <T> CompletableFuture<Void> sendAndReceiveToCluster(String type, T content){
        List<CompletableFuture<byte[]>> requests = new ArrayList<>();
        for (Address a : participants){
            requests.add(mms.sendAndReceive(a, type, s.encode(content)));
        }
        return CompletableFuture.allOf(requests.toArray(new CompletableFuture[0]));
    }

    //TODO pq não void?
    public CompletableFuture<Void> sendAsyncToCluster(String type, Object content) {
        System.out.println("sms:sendAsyncToCluster -> type == " + type);
        for (Address a : participants){
            mms.sendAsync(a, type, s.encode(content));
        }
        return CompletableFuture.completedFuture(null);
    }

    public <T> CompletableFuture<T> sendAndReceive(Address a, String type, Object content){
        return mms.sendAndReceive(a, type, s.encode(content),e)
                    .thenApply(b -> s.decode(b));
    }

    public void sendAndReceiveLoopToCluster(String type, Object content, int seconds, Consumer<Object> callback){
        System.out.println("sms:sendAndReceiveLoopToCluster -> type == " + type);
        for (Address a : participants){
            sendAndReceiveLoop(a,type,content,seconds,callback);
        }
    }

    public void sendAndReceiveLoop(Address a, String type, Object content, int seconds, Consumer<Object> callback){
        mms.sendAndReceive(a, type, s.encode(content), e)
                .whenComplete((message, throwable) ->{
                    if(throwable!=null){
                        //throwable.printStackTrace();
                        System.out.println("timeout resending msg with type +" + type + " to " + a);
                        Runnable task = () -> sendAndReceiveLoop(a, type,content,seconds, callback);
                        ses.schedule(task, seconds, TimeUnit.SECONDS);
                    }
                    else {
                        callback.accept(decode(message));
                    }
                });
    }


    public <T> CompletableFuture<Void> sendAsync(Address a, String type, T content){
        return mms.sendAsync(a,type,s.encode(content));
    }

    public CompletableFuture<Void> sendCausalOrderAsyncToCluster(String type, Object content) {
        System.out.println("sms:sendCausalOrderAsyncToCluster ->");
        byte[] toSend = coh.createMsg(content, type);
        for (Address a : participants){
            mms.sendAsync(a, type, toSend);
        }
        //TODO por ao fim de tudo allOf()...talvez
        coh.logAndSaveNonAckedOperation(toSend);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAndReceiveForRecovery(Duration timout){
        //List<CompletableFuture<Void>> cfs = new ArrayList<>();
        List<Integer> vector = coh.getVector();
        //TODO resolver
        System.out.println("sms:sendAndReceiveForRecovery ->");
        int i = 0;
        for(Address a : participants){
            MessageRecovery mr = new MessageRecovery(id, vector.get(i));
            mms.sendAndReceive(a, "causalOrderRecovery", s.encode(mr), timout, e)
                    .thenAccept(b -> System.out.println("sms:sendAndReceiveForRecovery -> " + (boolean)s.decode(b) + " by " + a));
            i++;
        }
        return CompletableFuture.completedFuture(null);
    }

    public void causalOrderRecover(Object msg, Consumer<Object> callback){
        coh.recoveryRead(encode(msg), callback);
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
