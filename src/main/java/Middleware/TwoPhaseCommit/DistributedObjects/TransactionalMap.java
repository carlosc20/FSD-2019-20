package Middleware.TwoPhaseCommit.DistributedObjects;

import Middleware.TwoPhaseCommit.Identifier;
import Middleware.TwoPhaseCommit.Participant;
import Middleware.TwoPhaseCommit.TransactionMessage;
import Middleware.TwoPhaseCommit.TransactionalObject;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class TransactionalMap<K,V>{
    private HashMap<K, TransactionalObject<V>> valuesById;
    private Participant participant;
    
    public TransactionalMap(Participant participant) {
        this.valuesById = new HashMap<>();
        this.participant =participant;
    }

    public CompletableFuture<Boolean> put(K key, V value){
        MapMessage<K,V> mm = new MapMessage<>(key, value);
        return participant.sendTransaction(mm);
    }

    public V get(K key){
        TransactionalObject<V> to = valuesById.get(key);
        if(to!=null && to.isCommited())
            return to.getObject();
        return null;
    }

    public boolean containsKey(K key){
        TransactionalObject<V> to = valuesById.get(key);
        return to != null && to.isCommited();
    }

    public void start() {
        System.out.println("dtm:regput -> starting");
        participant.startFirstPhase(firstPhaseAnswer);
        participant.startSecondPhase(isCommited, commit, abort);
    }

    private BiFunction<MapMessage<K,V>, Identifier, Boolean> firstPhaseAnswer = (mm, tid) ->{
        if(valuesById.containsKey(mm.key)){
            //caso seja um pedido doutra transação aborta
           if(!valuesById.get(mm.key).getTransactionId().equals(tid))
                return false;
           //caso seja um pedido repetido devolve a mesma resposta
           else return true;
        }
        else{
            TransactionalObject<V> to = new TransactionalObject<>(mm.value, tid);
            valuesById.put(mm.key, to);
            return true;
        }
    };

    private Function<MapMessage<K,V>, Boolean> isCommited = (mm) ->{
        if(!valuesById.containsKey(mm)) return false;
            if(valuesById.get(mm).isCommited())
                return true;
            return false;
    };

    private Consumer<MapMessage<K,V>> commit = (mm) -> {
        valuesById.get(mm.key).setCommited();
    };

    private BiConsumer<MapMessage<K,V>, Identifier> abort = (mm, tid) -> {
        //tem de a conter -> caso em que dá um abort repetido
        //e tem de ser da mesma transação -> caso em que temos 2 usernames iguais, o primeiro entra, e o segundo não
        //mas o abort do segundo não pode tirar o primeiro
        if(valuesById.containsKey(mm.key) && valuesById.get(mm.key).getTransactionId().equals(tid))
            valuesById.remove(mm.key);
    };

    public void transactionalRecover(Object obj){
        participant.recovery(firstPhaseAnswer, commit, abort, (TransactionMessage)obj);
    }
}
