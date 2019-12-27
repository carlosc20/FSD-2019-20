package Middleware.TwoPhaseCommit;

import io.atomix.utils.net.Address;

import java.util.*;

public class TransactionState {
    private Set<Address> states;
    private Object content;
    private int firstPhaseNotFinishedCounter;
    private int secondPhaseNotFinishedCounter;
    private char state;

    public TransactionState(List<Address> participants, Object content){
        this.state = 'p';
        this.content = content;
        this.states = new HashSet<>();
        for(Address a : participants)
            states.add(a);
        firstPhaseNotFinishedCounter = participants.size();
        secondPhaseNotFinishedCounter = participants.size();
    }

    public boolean insertAndAllAnsweredFirstPhase(Address a){
        if(!states.contains(a)){
            states.add(a);
            firstPhaseNotFinishedCounter--;
        }
        if(firstPhaseNotFinishedCounter == 0){
            states.clear();
            return true;
        }
        return false;
    }

    public boolean insertAndAllAnsweredSecondPhase(Address a){
        if(!states.contains(a)){
            states.add(a);
            secondPhaseNotFinishedCounter--;
        }
        return secondPhaseNotFinishedCounter == 0;
    }

    public void firstPhaseFinished(){
        this.firstPhaseNotFinishedCounter = 0;
    }

    public void setCommited() {
        this.state = 'c';
    }

    public boolean isAborted(){
        return state == 'a';
    }

    public  boolean isPrepared(){
        return  state == 'p';
    }

    public boolean isCommited(){
        return state == 'c';
    }

    public void setAborted() {
        this.state = 'a';
    }

    public int getFirstPhaseNotFinishedCounter() {
        return firstPhaseNotFinishedCounter;
    }

    public Object getContent(){
        return content;
    }

    public static void main(String[] args) {
        ArrayList<Address> servers = new ArrayList<>(); // encher
        int n = 3;
        int port = 10000;
        for(int i = 0; i < n; i++) {
            servers.add(Address.from(port + i));
        }
        //TransactionState ts = new TransactionState(servers);
        //System.out.println(ts.insertAndAllAnswered(servers.get(0)));
        //System.out.println(ts.insertAndAllAnswered(servers.get(1)));
        //System.out.println(ts.insertAndAllAnswered(servers.get(2)));
    }
}
