package Middleware.TwoPhaseCommit;

import Middleware.CausalOrder.CausalOrderHandler;
import Middleware.TwoPhaseCommit.OperationMessage;
import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


//TODO cuidado pq manager e participant tem executores diferentes
//TODO separar código de transações
public class Participant {
    private int id;
    private ExecutorService e;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;
    private Serializer s;
    private Address manager;
    private List<Address> participants;

    public Participant(int id, ManagedMessagingService mms, Serializer s, Address manager){
        this.id = id;
        this.e = Executors.newFixedThreadPool(1);
        this.mms = mms;
        this.coh = coh;
        this.manager = manager;
    }

    public CompletableFuture<TransactionMessage> begin(String name){
        TransactionMessage tm = new TransactionMessage(name);
        return mms.sendAndReceive(manager,"2pc", s.encode(tm),e)
                .thenApply((r) ->{
                    TransactionMessage tm2 = s.decode(r);
                    return tm2;
                });
    }

    public CompletableFuture<Void> sendOrderedOperation(String name, OperationMessage om){
        return sendAsyncToCluster(name, om);
    }

    public CompletableFuture<byte[]> commit(TransactionMessage tm){
        tm.setType('t');
        return mms.sendAndReceive(manager, "2pc", s.encode(tm), e);
    }

    public void registerOperation(String type){
        mms.registerHandler(type, (a,b)->{
            TransactionMessage tm = s.decode(b);
        },e);
    }

    public void registerOrderedOperation(String name, Consumer<OperationMessage> callback){
        mms.registerHandler(name, (a,b) -> {
            coh.read(b, o->{
                OperationMessage om = (OperationMessage) o;
                callback.accept(om);
            });
        },e);
    }
/*
    public void startTwoPhaseCommit(String name, Function<TransactionMessage, Integer> checkStatus){
        mms.registerHandler("2pc" + name, (a, b) -> {
            TransactionMessage tm = s.decode(b);
                switch (tm.getType()) {
                    case 'p':
                        tm.setType('p');
                        mms.sendAsync(a, "controller", s.encode(tm));
                        break;
                    case 'c':
                        //TODO logg
                        Object op = operations.get(tm.getTransactionId());
                        //callback.accept(op)
                        System.out.println("Logic.Server " + this.id + " Commited");
                        break;
                    case 'a':
                        //TODO logg
                        System.out.println("Logic.Server " + this.id + " Aborted");
                        break;
                }
        }, e);
    }
*/
    public CompletableFuture<Void> sendAsyncToCluster(String type, OperationMessage om) {
        byte[] toSend = coh.createMsg(om);
        for (Address a : participants)
            mms.sendAsync(a, type, toSend);
        return CompletableFuture.completedFuture(null);
    }
}
