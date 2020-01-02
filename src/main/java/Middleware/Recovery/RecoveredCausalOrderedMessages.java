package Middleware.Recovery;

import Middleware.CausalOrder.VectorMessage;

import java.util.ArrayList;
import java.util.List;

public class RecoveredCausalOrderedMessages {
    private int total;
    private int arrived;
    private List<VectorMessage> recoveredMessages;

    public RecoveredCausalOrderedMessages(int total){
        this.total = total;
        this.arrived = 0;
        this.recoveredMessages = new ArrayList<>(total);
    }

    public boolean add(VectorMessage vm){
        recoveredMessages.add(vm);
        arrived++;
        return arrived == total;
    }

    public List<VectorMessage> getRecoveredMessages(){
        return recoveredMessages;
    }
}
