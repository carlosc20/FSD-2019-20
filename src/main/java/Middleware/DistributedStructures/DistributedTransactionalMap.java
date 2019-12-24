package Middleware.DistributedStructures;

import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.Participant;
import Middleware.TwoPhaseCommit.TransactionalObject;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class DistributedTransactionalMap<K,V extends Mapped<K>> extends DistributedMap<K, TransactionalObject<V>>{
    private Participant p;
    private HashMap<Integer, TransactionalObject<V>> valuesByTransactionId;
    private HashMap<Integer, Integer> transactionsState; //1->prepared, 2->aborted
    
    public DistributedTransactionalMap(String name, ServerMessagingService sms, Participant p) {
        super(name, new HashMap<>(), sms);
        this.p = p;
        this.valuesByTransactionId = new HashMap<>();
        this.transactionsState = new HashMap<>();
    }

    //TODO mais métodos se for preciso

    public void registerDistributedTransactionalPut() {
        String operationName = this.name + ":put";
        sms.<OperationMessage<MapMessage<K,V>>>registerOperation(operationName, (om) ->{
            MapMessage<K,V> mm = om.getContent();
            if(!localContainsKey(mm.key)){
                TransactionalObject<V> to = new TransactionalObject<>(mm.value);
                localPut(mm.key, to);
                transactionsState.put(om.getTransactionId(), 1);
            }
            else transactionsState.put(om.getTransactionId(), 2);
        });
        p.listeningToTwoPhaseCommit(tm -> transactionsState.get(tm.getTransactionId()),
                tm -> {
                    int tid = tm.getTransactionId();
                    if(tm.getType() == 'c')
                        valuesByTransactionId.get(tid).setCommited();
                    else{
                        TransactionalObject<V> to = valuesByTransactionId.remove(tid);
                        localRemove(to.getObject().getKey());
                    }
                });
    }

    public CompletableFuture<Void> tPut(K key, V value){
        String operationName = this.name + ":put";
        return p.begin(operationName)
                    .thenAccept((tm) ->{
                        int tid = tm.getTransactionId();
                        TransactionalObject<V> to = new TransactionalObject<>(value);
                        OperationMessage om = put(key, to, tid);
                        sms.sendAsyncToCluster(operationName, om)
                            .thenAccept(x -> p.commit(tm));
                    });
    }


}
