package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageRecovery;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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

    public boolean getMissingOperations(MessageRecovery mr, Consumer<VectorMessage> callback){
        int senderClock = mr.getSavepoint();
        int numOp = nonAcknowledgedOperations.size();
        System.out.println("cohr:getMissingOperations -> sender clock " + senderClock);
        System.out.println("cohr:getMissingOperations -> number of unacked operations " + numOp);
        if(senderClock < numOp){
            for(int i = senderClock + 1; i <= numOp; i++){
                System.out.println("cohr:getMissingOperations -> sending operation with clock: " + i);
                System.out.println("cohr:getMissingOperations -> " + nonAcknowledgedOperations.get(i));
                callback.accept(nonAcknowledgedOperations.get(i));
            }
            return true;
        }
        return false;
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

