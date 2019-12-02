package Middleware.TwoPhaseCommit;

import io.atomix.utils.net.Address;

import java.util.ArrayList;
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
    public static void main(String[] args) {
        ArrayList<Address> servers = new ArrayList<>(); // encher
        int n = 3;
        int port = 10000;
        for(int i = 0; i < n; i++) {
            servers.add(Address.from(port + i));
        }
        TransactionState ts = new TransactionState(servers);
        System.out.println(ts.insertAndReadyToCommit(servers.get(0)));
        System.out.println(ts.insertAndReadyToCommit(servers.get(1)));
        System.out.println(ts.insertAndReadyToCommit(servers.get(2)));
    }
}
