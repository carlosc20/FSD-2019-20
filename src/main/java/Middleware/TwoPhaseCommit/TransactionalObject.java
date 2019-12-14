package Middleware.TwoPhaseCommit;

public class TransactionalObject<T> {
    private T object;
    private boolean commited;


    public TransactionalObject(T object){
        this.object = object;
        this.commited = false;
    }
}
