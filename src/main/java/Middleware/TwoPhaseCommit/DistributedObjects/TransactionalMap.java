package Middleware.TwoPhaseCommit.DistributedObjects;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.Participant;
import Middleware.TwoPhaseCommit.TransactionMessage;
import Middleware.TwoPhaseCommit.TransactionalObject;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class TransactionalMap<K,V>{
    private HashMap<K, TransactionalObject<V>> valuesById;
    private Participant participant;
    
    public TransactionalMap(Participant participant) {
        this.valuesById = new HashMap<>();
        this.participant =participant;
    }

    public CompletableFuture<byte[]> put(K key, V value){
        MapMessage<K,V> mm = new MapMessage<>(key, value);
        //para confirmar que o manager recebeu
        return participant.sendTransaction(mm);
    }

    public V get(K key){
        TransactionalObject<V> to = valuesById.get(key);
        if(to.isCommited())
            return to.getObject();
        return null;
    }

    public boolean containsKey(K key){
        TransactionalObject<V> to = valuesById.get(key);
        if(to!=null && to.isCommited())
            return true;
        return false;
    }

    public void start() {
        System.out.println("dtm:regput -> starting");
        participant.startFirstPhase(firstPhaseAnswer);
        participant.startSecondPhase(secondPhaseAnswer, commit, abort);
    }

    private Function<MapMessage<K,V>, Boolean> firstPhaseAnswer = (mm) ->{
        if(valuesById.containsKey(mm.key))
            return false;
        else{
            TransactionalObject<V> to = new TransactionalObject<>(mm.value);
            valuesById.put(mm.key, to);
            return true;
        }
    };

    private Function<MapMessage<K,V>, Boolean> secondPhaseAnswer = (mm) ->{
        if(valuesById.get(mm.key).isCommited())
            return false;
        return true;
    };

    private Consumer<MapMessage<K,V>> commit = (mm) -> {
        valuesById.get(mm.key).setCommited();
    };

    private Consumer<MapMessage<K,V>> abort = (mm) -> {
        valuesById.remove(mm.key);
    };

    public void transactionalRecover(Object obj){
        participant.recovery(firstPhaseAnswer, commit, abort, (TransactionMessage)obj);
    }

    public void sendRecoveryRequest(Duration d, Consumer<Object> callback){
        participant.sendRecoveryRequest(d,callback);
    }
}
