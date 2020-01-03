package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Recovery.MessageRecovery;
import io.atomix.utils.serializer.Serializer;

import java.util.*;


public class NeighboursRecoveryAssistant {
    //que este servidor enviou, Integer(meu clock)
    //guardar o que os outros servidores registam como o meu clock
    private int id;
    private Serializer s;
    private List<Integer> myClockOnCluster;
    private LinkedHashMap<Integer, VectorMessage> nonAcknowledgedOperations;
    private Logger log;

    public NeighboursRecoveryAssistant(int id, Serializer s, List<Integer> vector, Logger log) {
        this.id = id;
        this.s = s;
        this.myClockOnCluster = new ArrayList<>();
        myClockOnCluster.addAll(vector);
        this.nonAcknowledgedOperations = new LinkedHashMap<>();
        this.log = log;
    }

    public void saveUnackedOperation(VectorMessage vm){
        nonAcknowledgedOperations.put(vm.getIndex(this.id), vm);
    }

    public void updateClocks(VectorMessage vm, int myClock){
        int senderId = vm.getId();
        int clockValue = vm.getIndex(id);
        myClockOnCluster.set(senderId, clockValue);
        printArray(myClockOnCluster, "cohr:updateClocks -> Updated clocks value: ");
        int count=0;
        for(int i = 0; i<myClockOnCluster.size(); i++){
            if(i!=id){
                int c = myClockOnCluster.get(i);
                if(c >=clockValue)
                    count++;
            }
        }
        if(count==this.myClockOnCluster.size()-1){
            System.out.println("CLOCKVALUE " + clockValue);
            System.out.println("cohr:updateClocks -> acknowledging");
            Iterator<VectorMessage> iter = nonAcknowledgedOperations.values().iterator();
            while (iter.hasNext()) {
                VectorMessage msg = iter.next();
                if(msg.getIndex(this.id) <= clockValue)
                    iter.remove();
            }
        }
    }

    public MessageRecovery getMissingOperationMessage(){
        System.out.println("nonAck size = " + nonAcknowledgedOperations.size());
        int size = nonAcknowledgedOperations.size();
        if(size == 0) return new MessageRecovery(this.id, 0);
        int total = nonAcknowledgedOperations.get(size).getIndex(this.id);
        printMap("cohr:getMissingOperationMessage -> unAcked operations ");
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

    public void printMap(String header){
        StringBuilder strb = new StringBuilder();
        for(Object o : nonAcknowledgedOperations.values()){
            strb.append(o.toString()).append('\n');
        }
        System.out.println(header + strb);
    }
}

