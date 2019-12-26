package Middleware.DistributedStructures;

import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.Participant;
import Middleware.TwoPhaseCommit.TransactionMessage;
import Middleware.TwoPhaseCommit.TransactionalObject;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class DistributedTransactionalMap<K,V extends Mapped<K>>{
    private Participant p;
    private HashMap<K, TransactionalObject<V>> valuesById;
    private HashMap<Integer, TransactionalObject<V>> valuesByTransactionId;
    private HashMap<Integer, Integer> transactionsState; //1->prepared, 2->aborted
    private String name;
    private ServerMessagingService sms;
    
    public DistributedTransactionalMap(String name, ServerMessagingService sms, Participant p) {
        this.name = name;
        this.valuesById = new HashMap<>();
        this.sms = sms;
        this.p = p;
        this.valuesByTransactionId = new HashMap<>();
        this.transactionsState = new HashMap<>();
        registerDistributedTransactionalPut();
    }

    //TODO mais métodos se for preciso
    //TODO lógica de cliente...tipo get e ver se está commited
    //DEBUG
    public void localPut(K key, V value){
        TransactionalObject<V> to = new TransactionalObject<>(value);
        valuesById.put(key, to);
    }

    public void registerDistributedTransactionalPut() {
        String operationName = this.name + ":put";
        System.out.println("dtm:regput -> starting");
        sms.<OperationMessage<MapMessage<K,V>> >registerOperation(operationName, om ->{
            System.out.println("dtm:regput -> put request arrived transactionId == " + om.getTransactionId());
            MapMessage<K,V> mm = om.getContent();
            return put(mm.key, om.getTransactionId(), mm.value);
        });
        p.listeningToFirstPhase(tm -> transactionsState.get(tm.getTransactionId()));
        p.listeningToSecondPhase(secondPhase );
    }

    private Consumer<TransactionMessage> secondPhase = (tm) ->{
        int tid = tm.getTransactionId();
        if (tm.getType() == 'c'){
            System.out.println("dtm:regput -> Transaction id==" + tm.getTransactionId() + "status == commited");
            //TODO deve funcionar mudar por referencia e tirar do outro map
            valuesByTransactionId.get(tid).setCommited();
            valuesByTransactionId.remove(tid);
        }
        else {
            System.out.println("dtm:regput -> Transaction id==" + tm.getTransactionId() + "status == aborted");
            if(valuesByTransactionId.containsKey(tid)){
                TransactionalObject<V> to = valuesByTransactionId.remove(tid);
                valuesById.remove(to.getObject().getKey());
            }
        }
        transactionsState.remove(tid);
    };


    public CompletableFuture<Void> put(K key, V value){
        String operationName = this.name + ":put";
        return p.begin(operationName)
                    .thenAccept((tm) ->{
                        int tid = tm.getTransactionId();
                        //TODO muitos new ...
                        put(key, tid, value);
                        MapMessage<K,V> mm = new MapMessage<>(key, value);
                        OperationMessage<MapMessage<K,V>> om = new OperationMessage<>(tid,mm,operationName);
                        System.out.println("dtm:tPut -> sending " + om.toString());
                        sms.sendAndReceiveToCluster(operationName, om)
                            .thenAccept(x -> {
                                System.out.println("sending commit to manager");
                                p.commit(tm);});
                    });
    }

    //TODO retorna algo para fazer sendAndReceive?
    private int put(K key, Integer transactionId, V value){
        TransactionalObject<V> to = new TransactionalObject<>(value);
        if(!valuesById.containsKey(key)){
            System.out.println("dtm:regput -> key validated! tid == " + transactionId);
            valuesById.put(key, to);
            valuesByTransactionId.put(transactionId, to);
            transactionsState.put(transactionId, 1);
            return 1;
        }
        //ASSIM por causa do caso de retirar tudo só porque deu abort
        else {
            System.out.println("dtm:regput -> key already exists! tid == " + transactionId);
            transactionsState.put(transactionId, 2);
            return 2;
        }
    }


}
