package Middleware.TwoPhaseCommit;

import Middleware.CausalOrder.CausalOrderHandler;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.serializer.Serializer;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


//TODO cuidado pq manager e participant tem executores diferente
public class Participant {
    private int id;
    private ExecutorService e;
    private ManagedMessagingService mms;
    private CausalOrderHandler coh;
    private Serializer s;
    private Map<Identifier, Object> operations;

    public Participant(int id, ManagedMessagingService mms, CausalOrderHandler coh, Serializer s){
        this.id = id;
        this.e = Executors.newFixedThreadPool(1);
        this.operations = new HashMap<>();
        this.mms = mms;
        this.coh = coh;
    }

    public void registerOrderedOperation(String type){
        coh.registerHandler(type, o->{
            TransactionMessage tm = (TransactionMessage) o;
            //TODO log
            operations.put(tm.getTransactionId(), tm.getContent());
        },e);
    }

    public void registerOperation(String type){
        mms.registerHandler(type, (a,b)->{
            TransactionMessage tm = s.decode(b);
            //TODO log
            operations.put(tm.getTransactionId(), tm.getContent());
        },e);
    }

    public void startTwoPhaseCommit(Consumer<Object> callback) {
        mms.registerHandler("2pc", (a,b) -> {
            TransactionMessage tm = s.decode(b); //passou-se isto
            switch (tm.getType()) {
                case 'p':
                    tm.setType('p');
                    mms.sendAsync(a, "controller", s.encode(tm));
                    break;
                case 'c':
                    //TODO logg
                    System.out.println("Logic.Server " + this.id +" Commited");
                    break;
                case 'a':
                    //TODO logg
                    System.out.println("Logic.Server " + this.id +" Aborted");
                    break;
            }
        }, e);
    }
}
