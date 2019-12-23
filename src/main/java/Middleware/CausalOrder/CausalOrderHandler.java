package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Marshalling.MessageRecovery;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

//TODO tirar endereços nas vector messages.

public class CausalOrderHandler {

    private int id;
    private List<Integer> vector;
    private Queue<VectorMessage> msgQueue;
    private Serializer s;
    private COHRecovery cohr;


    public CausalOrderHandler(int id, int clusterSize, Serializer s, Logger log){
        this.id = id;
        this.vector = new ArrayList<>();
        for(int i = 0; i<clusterSize; i++){
            this.vector.add(0);
        }
        this.msgQueue = new LinkedList<>();
        this.s = s;
        this.cohr = new COHRecovery(id, s, vector, log);
    }
/*
    public List<Integer> recover(Consumer<Object> serverCallback){
        Object oldVector = cohr.recoverVector();
        if(oldVector != null){
            System.out.println("coh:recover -> vector in loggs is not null");
            this.vector = (List<Integer>)oldVector;
        }
        printArray(vector, "coh:recover -> vector: ");
        cohr.recoverMessageQueue((obj)->{
            VectorMessage vm = (VectorMessage) obj;
            if(!inOrder(vm)){
                this.msgQueue.add(vm);
            }
        });
        cohr.recoverOperations(serverCallback);
        return vector;
    }
*/
    public boolean treatRecoveryRequest(MessageRecovery mr, Consumer<VectorMessage> callback){
        return cohr.getMissingOperations(mr, callback);
    }

    public void read(int type, byte[] b, Consumer<Object> callback){
        VectorMessage msg = s.decode(b);
        System.out.println("coh:read"+type+ "-> Received a msg "+ msg.toString());
        System.out.println("coh:read"+type+ "-> from server " + msg.getId() + " and I have " + vector.get(msg.getId())+ " as is clock");
        if(type == 0)
            cohr.logOrderedOperation(msg);
        if(inOrder(msg)){
            System.out.println("coh:read"+type+ "->inOrder");
            updateVector(msg);
            cohr.updateClocks(msg);
            callback.accept(msg.getContent());
            updateQueue(type, callback);
        }
        else{
            System.out.println("coh:read"+type+ "->outOfOrder");
            msgQueue.add(msg);
        }
    }

    private void updateQueue(int type, Consumer<Object> callback){
        Iterator<VectorMessage> iter = msgQueue.iterator();
        while (iter.hasNext()){
            VectorMessage msg = iter.next();
            if(inOrder(msg)){
                updateVector(msg);
                cohr.updateClocks(msg);
                if(type == 0)
                    cohr.logOrderedOperation(msg);
                //TODO ack messages
                callback.accept(msg.getContent());
                iter.remove();
                updateQueue(type, callback);
                return;
            }
        }
    }
    private void updateVector(VectorMessage msg){
        int id = msg.getId();
        vector.set(id, msg.getIndex(id));
    }

    private boolean inOrder(VectorMessage msg){
        List<Integer> v = msg.getVector();
        int id = msg.getId();
        for(int i=0; i < v.size(); i++){
            int local = vector.get(i);
            int other = v.get(i);
            if(i != id){
                if(local < other){
                    return false;
                }
            }
            else if(local + 1 != other){
                return false;
            }
        }
        return true;
    }

    public byte[] createMsg(Object content) {
        vector.set(id, vector.get(id) + 1); // incrementa vetor local
        VectorMessage vm = new VectorMessage(id, vector, content);
        System.out.println(vm.toString());
        //TODO cuidado
        //TODO add to unacknoledged
        return s.encode(vm);
    }

    public void logAndSaveNonAckedOperation(byte[] toSend){
        System.out.println("Logging operation");
        VectorMessage vm = s.decode(toSend);
        cohr.saveUnackedOperation(vector.get(id),vm);
        cohr.logOrderedOperation(vm);
    }

    public List<Integer> getVector() {
        return vector;
    }

    //DEBUG
    private void printArray(List<Integer> v, String header){
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(Integer.toString(i)).append('/');
        }
        System.out.println(header + strb);
    }


    public static void main(String[] args) throws InterruptedException {
      /*
        Consumer<VectorMessage> cvm = (msg)-> System.out.println(msg.getContent());
        CausalOrderHandler coh = new CausalOrderHandler(0,2);

        ArrayList<Integer> a1 = new ArrayList<>();
        ArrayList<Integer> a2 = new ArrayList<>();
        ArrayList<Integer> a3 = new ArrayList<>();

        for(int i = 0; i<2; i++){
            a1.add(0);
            a2.add(0);
            a3.add(0);
        }

        VectorMessage<String> vm1 = new VectorMessage<>(1,a1, "olá");
        VectorMessage<String> vm2 = new VectorMessage<>(1,a2, "está tudo bem");
        VectorMessage<String> vm3 = new VectorMessage<>(1,a3, "adeus");

        vm1.setIndex(1,1);
        vm2.setIndex(1,2);
        vm3.setIndex(1,3);

        coh.read(vm3, cvm);
        Thread.sleep(2000);
        coh.read(vm2, cvm);
        Thread.sleep(2000);
        coh.read(vm1, cvm);
/*
   //Bloco de código concorrente! Métodos desta classe não suportam concorrência. Meter synchronized para testar
        for(int i=1; i<=100; i++){
            VectorMessage vm = new VectorMessage();
            vm.setIndex(1,i);
            new Thread(()-> {
                try {
                   Thread.sleep(100 * r.nextInt(high - low) + low);
                   coh.read(vm, cvm);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
           }).start();
        }
        while(true)
            Thread.sleep(1000);
    */
    }
}
