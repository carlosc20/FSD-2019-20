package Middleware.TwoPhaseCommit;

import io.atomix.utils.net.Address;

import java.util.*;

public class TransactionState {
    private Set<Address> states;
    private int notReadyCounter;
    private int type; //0->unCommited, 1->commited, 2->aborted

    public TransactionState(List<Address> participants){
        this.type = 0;
        this.states = new HashSet<>();
        for(Address a : participants)
            states.add(a);
        notReadyCounter = participants.size();
    }

    public boolean insertAndReadyToCommit(Address a){
        if(!states.contains(a)){
            states.add(a);
            notReadyCounter--;
        }
        return notReadyCounter == 0;
    }

    public void setCommited() {
        this.type = 1;
    }

    public void setAborted() {
        this.type = 2;
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
