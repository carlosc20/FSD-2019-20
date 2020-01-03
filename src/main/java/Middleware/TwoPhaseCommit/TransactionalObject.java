package Middleware.TwoPhaseCommit;

public class TransactionalObject<V>{
    private V object;
    private boolean state; //true == commited, false == nonCommited
    private Identifier transactionId;


    public TransactionalObject(V object, Identifier transactionId){
        this.object = object;
        this.state = false;
        this.transactionId = transactionId;
    }

    public void setCommited(){
        this.state= true;
    }

    public boolean isCommited(){return this.state;}

    public Identifier getTransactionId() {
        return transactionId;
    }

    public V getObject() {
        return object;
    }

    @Override
    public String toString() {
        return "TransactionalObject{" +
                "object=" + object +
                ", state=" + state +
                '}';
    }

}
