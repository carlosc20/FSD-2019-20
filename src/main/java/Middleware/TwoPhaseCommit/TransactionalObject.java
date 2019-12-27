package Middleware.TwoPhaseCommit;

import Middleware.DistributedStructures.Mapped;

public class TransactionalObject<V extends Mapped> implements Mapped{
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

    public Object getKey(){
        return object.getKey();
    }

    @Override
    public String toString() {
        return "TransactionalObject{" +
                "object=" + object +
                ", state=" + state +
                '}';
    }

}
