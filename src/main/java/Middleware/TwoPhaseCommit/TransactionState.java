package Middleware.TwoPhaseCommit;

import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionState {
    private Map<Address,Boolean> states;
    private int notReadyCounter;

    public TransactionState(List<Address> participants){
        this.states = new HashMap<>();
        for(Address a : participants)
            states.put(a,false);
        notReadyCounter = participants.size();
    }

    public boolean insertAndReadyToCommit(Address a){
        //para precaver
        if(states.containsKey(a)){
            if(!states.get(a)){
                states.put(a,true);
                notReadyCounter--;
            }
        }
        return notReadyCounter == 0;
    }
}
