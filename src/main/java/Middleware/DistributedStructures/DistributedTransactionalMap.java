package Middleware.DistributedStructures;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.TransactionMessage;
import Middleware.TwoPhaseCommit.TransactionalObject;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class DistributedTransactionalMap<K,V extends Mapped<K>>{
    private Address manager;
    private HashMap<K, TransactionalObject<V>> valuesById;
    private String name;
    private ServerMessagingService sms;
    private Logger log;
    
    public DistributedTransactionalMap(String name, ServerMessagingService sms, Address manager, Logger log) {
        this.manager = manager;
        this.name = name;
        this.valuesById = new HashMap<>();
        this.sms = sms;
        this.log = log;
        start();
    }

    public CompletableFuture<byte[]> put(K key, V value){
        MapMessage<K,V> mm = new MapMessage<>(key, value);
        //para confirmar que o manager recebeu
        return sendTransaction(mm);
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

    /*
    public CompletableFuture<byte[]> remove(K key, V value){
        //TODO

    }
*/
    public void start() {
        System.out.println("dtm:regput -> starting");
        sms.registerOperation("firstphase", firstPhase);
        sms.registerOperation("secondphase", secondPhase);
    }

    private Consumer<TransactionMessage<MapMessage<K,V>>> firstPhase = (tm) -> {
        System.out.println("dtm:firstphasereg -> received prepared request + " + tm.toString());
        MapMessage<K,V> mm = tm.getContent();
        if(valuesById.containsKey(mm.key)){
            System.out.println("dtm:firstphasereg -> key already exists");
            tm.setAborted();
        }
        else{
            System.out.println("dtm:firstphasereg -> value will be inserted");
            tm.setPrepared();
            TransactionalObject<V> to = new TransactionalObject<>(mm.value);
            valuesById.put(mm.key, to);
        }
        log.write(tm);
        sms.sendAsync(manager, "firstphase", tm);
    };

    private Consumer<TransactionMessage<MapMessage<K,V> >> secondPhase = (tm) ->{
        System.out.println("dtm:secondphasereg -> received prepared request + " + tm.toString());
        log.write(tm);
        MapMessage<K,V> mm = tm.getContent();
        if (tm.isCommited()){
            System.out.println("dtm:secondphasereg -> Transaction id== " + tm.getTransactionId() + " commited");
            valuesById.get(mm.key).setCommited();
        }
        else {
            System.out.println("dtm:secondphasereg-> Transaction id== " + tm.getTransactionId() + " aborted");
            valuesById.remove(mm.key);
        }
    };

    private CompletableFuture<byte[]> sendTransaction(MapMessage<K,V> mm){
        TransactionMessage<MapMessage<K,V>> tm = new TransactionMessage<>(mm);
        System.out.println("dtm:sendTransaction -> commiting transaction");
        return sms.sendAndReceive(manager, "startTransaction", tm);
    }

    public void recover(HashMap<Integer, TransactionMessage<MapMessage<K,V>>> tms){
        int maxId = 0;
        for(TransactionMessage<MapMessage<K,V>> tm : tms.values()){
            maxId = Integer.max(maxId, tm.getTransactionId());
            if(tm.isCommited()){
                MapMessage<K,V> mm = tm.getContent();
                TransactionalObject<V> to = new TransactionalObject<>(mm.value);
                to.setCommited();
                valuesById.put(mm.key, to);
                //porque ele não sabe se enviou a mensagem ou não
                sms.sendAsync(manager, "secondphase", tm);
            }
            else if(tm.isPrepared()){
                MapMessage<K,V> mm = tm.getContent();
                TransactionalObject<V> to = new TransactionalObject<>(mm.value);
                valuesById.put(mm.key, to);
                sms.sendAsync(manager, "firstphase", tm);
            }
            else {
                sms.sendAsync(manager, "firstphase", tm);
            }
        }
        //pedido dos que faltam, resulta porque o manager usa um lock e faz sequencial o envio (Assumo eu)
        sms.sendAndReceive(manager, "recovery", maxId+1);
    }

    /*
    public CompletableFuture<Void> put(K key, V value){
        String operationName = this.name + ":put";
        return begin(operationName)
                    .thenAccept((tid) ->{
                        put(key, tid, value);
                        MapMessage<K,V> mm = new MapMessage<>(key, value);
                        TransactionMessage<MapMessage<K,V>> tm = new TransactionMessage<>(tid,operationName,mm);
                        log.write(tm);
                        System.out.println("dtm:tPut -> sending " + tm.toString());
                        sms.sendAndReceiveToCluster(operationName, tm)
                            .thenAccept(x -> {
                                System.out.println("sending commit to manager");
                                commit(tm);});
                    });
    }
*/
}
