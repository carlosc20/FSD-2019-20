package Middleware.TwoPhaseCommit.DistributedObjects;

import Middleware.Logging.Logger;
import Middleware.ServerMessagingService;
import Middleware.TwoPhaseCommit.TransactionMessage;
import Middleware.TwoPhaseCommit.TransactionalObject;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class TransactionalMap<K,V extends Mapped<K>>{
    private Address manager;
    private HashMap<K, TransactionalObject<V>> valuesById;
    private ServerMessagingService sms;
    private Logger log;
    
    public TransactionalMap(ServerMessagingService sms, Address manager, Logger log) {
        this.manager = manager;
        this.valuesById = new HashMap<>();
        this.sms = sms;
        this.log = log;
        transactionsRecovery();
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
        sms.sendAsync(manager, "secondphase", tm);
    };

    private CompletableFuture<byte[]> sendTransaction(MapMessage<K,V> mm){
        TransactionMessage<MapMessage<K,V>> tm = new TransactionMessage<>(mm);
        System.out.println("dtm:sendTransaction -> commiting transaction");
        return sms.sendAndReceive(manager, "startTransaction", tm);
    }

    private void transactionsRecovery(){
        HashMap<Integer, TransactionMessage> tms = getTransactions();
        int maxId = 0;
        for(TransactionMessage tm : tms.values()){
            maxId = Integer.max(maxId, tm.getTransactionId());
            recover.accept(tm);
        }
        //pedido dos que faltam, resulta porque o manager usa um lock e faz sequencial o envio (Assumo eu)
        sms.sendAndReceive(manager, "transactionalRecovery", maxId+1);
    }

    private HashMap<Integer, TransactionMessage> getTransactions() {
        HashMap<Integer, TransactionMessage> auxiliar = new HashMap<>();
        log.recover( (msg)->{
            if(msg instanceof TransactionMessage){
                TransactionMessage tm = (TransactionMessage) msg;
                auxiliar.put(tm.getTransactionId(), tm);
            }
        });
        return auxiliar;
    }

    private Consumer<TransactionMessage<MapMessage<K,V>>> recover = (tm) ->{
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
    };
}
