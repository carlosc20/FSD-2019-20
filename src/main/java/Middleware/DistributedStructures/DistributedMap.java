package Middleware.DistributedStructures;

import Middleware.ServerMessagingService;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
@Deprecated
public class DistributedMap<K,V> {
    String name;
    ServerMessagingService sms;
    private HashMap<K, V> valuesById;

    public DistributedMap(){
        this.name = "";
        this.valuesById = new HashMap<>();
    }

    public DistributedMap(String name, HashMap<K, V> valuesById, ServerMessagingService sms){
        this.name = name;
        this.valuesById = valuesById;
        this.sms = sms;
    }

    public void registerDistributedPut(){
        String operationName = name + ":put";
        sms.<MapMessage<K,V>>registerOperation(operationName, mm-> {valuesById.put(mm.key, mm.value);});
    }

    public void registerDistributedRemove(){
        String operationName = name + ":remove";
        sms.<MapMessage<K,V>>registerOperation(operationName, mm-> {valuesById.remove(mm.key);});
    }

    public CompletableFuture<Void> put(K key, V value){
        String operationName = name + ":put";
        valuesById.put(key,value);
        //TODO isto tem muito new
        MapMessage<K,V> mm = new MapMessage<>(key, value);
        OperationMessage<MapMessage<K,V>> om = new OperationMessage<>(mm,operationName);
        return sms.sendAsyncToCluster(operationName, om);
    }

    public CompletableFuture<Void> remove(K key){
        String operationName = name + ":remove";
        valuesById.remove(key);
        //TODO ver este null
        MapMessage<K,V> mm = new MapMessage<>(key, null);
        OperationMessage<MapMessage<K,V>> om = new OperationMessage<>(mm,operationName);
        sms.sendAsyncToCluster(operationName, om);
        return CompletableFuture.completedFuture(null);
    }

    protected V localPut(K key, V value){
        return valuesById.put(key,value);
    }

    protected boolean localContainsKey(K key){
        return valuesById.containsKey(key);
    }

    protected void localRemove(K key){
        this.valuesById.remove(key);
    }
}
