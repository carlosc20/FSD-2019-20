package Middleware.TwoPhaseCommit.TransactionalStructures;

public class MapMessage<K,V> {
    K key;
    V value;

    public MapMessage(K key, V value){
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "MapMessage{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
