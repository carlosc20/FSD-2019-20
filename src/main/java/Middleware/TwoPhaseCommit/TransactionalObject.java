package Middleware.TwoPhaseCommit;

public class TransactionalObject<V>{
    private V object;
    private boolean state; //true == commited, false == nonCommited


    public TransactionalObject(V object){
        this.object = object;
        this.state = false;
    }

    public void setCommited(){
        this.state= true;
    }

    public boolean isCommited(){return this.state;}

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
