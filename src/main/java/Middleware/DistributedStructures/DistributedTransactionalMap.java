package Middleware.DistributedStructures;

import Middleware.Logging.Logger;
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
    private Logger log;
    
    public DistributedTransactionalMap(String name, ServerMessagingService sms, Participant p, Logger log) {
        this.name = name;
        this.valuesById = new HashMap<>();
        this.sms = sms;
        this.p = p;
        this.valuesByTransactionId = new HashMap<>();
        this.transactionsState = new HashMap<>();
        //para sair
        registerDistributedTransactionalPut();
        this.log = log;
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
        sms.<TransactionMessage<MapMessage<K,V>> >registerOperation(operationName, tm ->{
            System.out.println("dtm:regput -> put request arrived transactionId == " + tm.getTransactionId());
            MapMessage<K,V> mm = tm.getContent();
            log.write(tm);
            return put(mm.key, tm.getTransactionId(), mm.value);
        });
        p.listeningToFirstPhase(tm -> transactionsState.get(tm.getTransactionId()));
        p.listeningToSecondPhase(secondPhase);
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
                    .thenAccept((tid) ->{
                        //TODO muitos new ...
                        put(key, tid, value);
                        MapMessage<K,V> mm = new MapMessage<>(key, value);
                        TransactionMessage<MapMessage<K,V>> tm = new TransactionMessage<>(tid,operationName,mm);
                        log.write(tm);
                        System.out.println("dtm:tPut -> sending " + tm.toString());
                        sms.sendAndReceiveToCluster(operationName, tm)
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

    public V get(K key){
        return valuesById.get(key).getObject();
    }

    public boolean containsKey(K key){
        return valuesById.containsKey(key);
    }


}
