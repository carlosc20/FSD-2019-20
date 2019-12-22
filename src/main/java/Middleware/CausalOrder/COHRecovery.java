package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageRecovery;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class COHRecovery {
    //TODO
    //que este servidor enviou, Integer(meu clock)
    //guardar o que os outros servidores registam como o meu clock
    private int id;
    private Serializer s;
    private List<Integer> myClockOnCluster;
    private HashMap<Integer, VectorMessage> unacknowledgedOperations;
    private Logger logMyClockOnCluster;
    private Logger logVector;
    private Logger logInOrderOperations;
    private Logger logOutOfOrderOperations;

    public COHRecovery(int id, Serializer s, List<Integer> vector) {
        this.id = id;
        this.s = s;
        this.myClockOnCluster = new ArrayList<>();
        myClockOnCluster.addAll(vector);
        this.unacknowledgedOperations = new HashMap<>();
        //TODO estes dois loggs talvez possam ser otimizados
        this.logMyClockOnCluster = new Logger("causalOrderLogs", "Server" + id + "-ClockOnCluster", s);
        this.logVector = new Logger("causalOrderLogs", "Server" + id + "-Vector", s);
        this.logInOrderOperations = new Logger("causalOrderLogs", "Server" + id + "-InOrderOps", s);
        this.logOutOfOrderOperations = new Logger("causalOrderLogs", "Server" + id + "-OutOfOrder", s);
    }

    public Object recoverVector() {
        return logVector.recoverLast();
    }

    public void saveUnackedOperation(int clock, VectorMessage vm){
        unacknowledgedOperations.put(clock, vm);
        System.out.println("saveUnackedOperation -> size: " + unacknowledgedOperations.size());
    }

    public void recoverMessageQueue(Consumer<Object> callback) {
        logOutOfOrderOperations.recover(callback);
    }

    public void recoverOperations(Consumer<Object> serverCallback) {
        System.out.println("Recovering Operations");
        Object oldMyClockOnCluster = logMyClockOnCluster.recoverLast();
        if(oldMyClockOnCluster != null)
            this.myClockOnCluster = (List<Integer>)oldMyClockOnCluster;
        printArray(myClockOnCluster, "recoverOperation -> cohr clock recovered: ");
        int min = Collections.min(myClockOnCluster);
        System.out.println("recoverOperation -> Minimum obtained: "+min);
        logInOrderOperations.recover((obj) -> {
            VectorMessage vm = (VectorMessage) obj;
            int clock = vm.getIndex(id);
            if (vm.getId() == id && clock > min) {
                System.out.println("recoverOperation -> Vector message not acknowledged: " + vm.toString());
                unacknowledgedOperations.put(clock, vm);
            }
            serverCallback.accept(vm.getContent());
        });
    }

    public void updateClocks(VectorMessage vm){
        System.out.println("updateClocks -> updating cohr clocks");
        int senderId = vm.getId();
        int clockValue = vm.getIndex(id);
        myClockOnCluster.set(senderId, clockValue);
        logMyClockOnCluster();
        printArray(myClockOnCluster, "updateClocks -> Updated cohr clocks value: ");
        int count=0;
        for(Integer i : myClockOnCluster){
            if(i >= clockValue)
                count++;
        }
        System.out.println("updateClocks -> Number of acks: " + count);
        System.out.println("updateClocks -> struct size " + myClockOnCluster.size());
        if(count==this.myClockOnCluster.size())
            unacknowledgedOperations.remove(clockValue);
    }

    public boolean getMissingOperations(MessageRecovery mr, Consumer<byte[]> callback){
        int senderClock = mr.getClock();
        int numOp = unacknowledgedOperations.size();
        System.out.println("getMissingOperations -> sender clock " + senderClock);
        System.out.println("getMissingOperations- > number of unacked operations " + numOp);
        if(senderClock < numOp){
            for(int i = senderClock; i <= numOp; i++){
                callback.accept(s.encode(unacknowledgedOperations.get(i)));
            }
            return true;
        }
        return false;
    }

    public void logVector(List<Integer> vector){
        printArray(vector, "Logging vector: " );
        this.logVector.write(vector);
    }

    public void logMyClockOnCluster(){
        printArray(myClockOnCluster, "Logging clock on cluster: ");
        this.logMyClockOnCluster.write(this.myClockOnCluster);
    }

    public void logInOrderOperation(VectorMessage vm){
        this.logInOrderOperations.write(vm);
    }

    public void logOutOfOrderOperation(VectorMessage vm){
        this.logOutOfOrderOperations.write(vm);
    }

    //DEBUG
    private void printArray(List<Integer> v, String header){
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(Integer.toString(i)).append('/');
        }
        System.out.println(header + strb);
    }

}

