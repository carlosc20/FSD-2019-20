package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Recovery.MessageRecovery;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class NeighboursRecoveryAssistant {
    //TODO
    //que este servidor enviou, Integer(meu clock)
    //guardar o que os outros servidores registam como o meu clock
    private int id;
    private Serializer s;
    private List<Integer> myClockOnCluster;
    private HashMap<Integer, VectorMessage> nonAcknowledgedOperations;
    private Logger log;

    public NeighboursRecoveryAssistant(int id, Serializer s, List<Integer> vector, Logger log) {
        this.id = id;
        this.s = s;
        this.myClockOnCluster = new ArrayList<>();
        myClockOnCluster.addAll(vector);
        this.nonAcknowledgedOperations = new HashMap<>();
        this.log = log;
    }

    public void saveUnackedOperation(int clock, VectorMessage vm){
        nonAcknowledgedOperations.put(clock, vm);
        System.out.println("cohr:saveUnackedOperation -> Saving " + nonAcknowledgedOperations.get(clock));
        printMap("cohr:saveUnackedOperation -> hashmap updated");
    }

    public void updateClocks(VectorMessage vm){
        int senderId = vm.getId();
        int clockValue = vm.getIndex(id);
        myClockOnCluster.set(senderId, clockValue);
        printArray(myClockOnCluster, "cohr:updateClocks -> Updated clocks value: ");
        int count=0;
        for(Integer i : myClockOnCluster){
            if(i >= clockValue)
                count++;
        }
        System.out.println("cohr:updateClocks -> Number of acks: " + count);
        if(count==this.myClockOnCluster.size())
            System.out.println("cohr:updateClocks -> acknowledging");
            nonAcknowledgedOperations.remove(clockValue);
    }

    public MessageRecovery getMissingOperationMessage(List<Integer> vector){
        int savepoint = myClockOnCluster.get(vector.get(this.id));
        int total = nonAcknowledgedOperations.size();
        System.out.println("cohr:getMissingOperations -> sender clock " + savepoint);
        System.out.println("cohr:getMissingOperations -> total number of unacked operations " + total);
        return new MessageRecovery(this.id, total);
    }

    public VectorMessage getVectorMessage(int messageid){
        return nonAcknowledgedOperations.get(messageid);
    }

    public void logOrderedOperation(VectorMessage vm){
        System.out.println("cohr:logInOrderOperation -> logging ordered operation");
        this.log.write(vm);
    }

    //DEBUG
    private void printArray(List<Integer> v, String header){
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(Integer.toString(i)).append('/');
        }
        System.out.println(header + strb);
    }

    private void printMap(String header){
        StringBuilder strb = new StringBuilder();
        for(Object o : nonAcknowledgedOperations.values()){
            strb.append(o.toString()).append('\n');
        }
        System.out.println(header + strb);
    }
}

