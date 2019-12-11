package Middleware.TwoPhaseCommit;

@Deprecated
public class MyConsumer<T, U, V> {

    private V callbackId;
    private TriConsumer<T, U, V> transactionConsumer;

    public MyConsumer(V callbackId, TriConsumer<T, U, V> transactionConsumer){
        this.callbackId = callbackId;
        this.transactionConsumer = transactionConsumer;
    }

    public void accept(T t, U u, V v){
        transactionConsumer.accept(t,u,v);
    }
}
