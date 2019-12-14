package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

//TODO meter endereços nas vector messages.

public class CausalOrderHandler {

    private int id;
    private List<Integer> vector;
    private Queue<VectorMessage> msgQueue;
    private Serializer s;
    private Logger logVector;
    private Logger logQueue;

    public CausalOrderHandler(int id, int clusterSize, Serializer s){
        this.id = id;
        this.vector = new ArrayList<>();
        for(int i = 0; i<clusterSize; i++){
            this.vector.add(0);
        }
        this.msgQueue = new LinkedList<>();
        this.s = s;
        this.logVector = new Logger("causalOrderLogs","Vector", s);
        this.logQueue = new Logger("causalOrderLogs","Queue", s);
    }

    public List<Integer> recover(){
        ArrayList<Object> vectors = logVector.recover();
        if(vectors.size() != 0){
            ArrayList<Integer> vector = (ArrayList<Integer>) vectors.get(vectors.size() - 1);
            this.vector = vector;
            ArrayList<Object> queue = logQueue.recover();
            recoverQueue(queue);
        }
        return vector;
    }

    private void recoverQueue(ArrayList<Object> queue){
        Iterator<Object> it = queue.iterator();
        while(it.hasNext()){
            VectorMessage vm = (VectorMessage) it.next();
            if(!inOrder(vm)){
                this.msgQueue.add(vm);
            }
        }
    }

    public void read(byte[] b, Consumer<Object> callback){
        VectorMessage msg = s.decode(b);
        System.out.println(id + ": Received a msg "+ msg.toString());
        System.out.println(id + ": from: " + msg.getId() + " i have: " + vector.get(msg.getId()));
        if(inOrder(msg)){
            System.out.println(id + ": inOrder");
            updateVector(msg);
            callback.accept(msg.getContent());
            updateQueue(callback);
        }
        else{
            System.out.println(id + ": outOfOrder");
            logQueue.write(msg);
            msgQueue.add(msg);
        }
    }

    private void updateVector(VectorMessage msg){
        int id = msg.getId();
        vector.set(id, msg.getIndex(id));
        logVector.write(vector);
    }

    private void updateQueue(Consumer<Object> callback){
        Iterator<VectorMessage> iter = msgQueue.iterator();
        while (iter.hasNext()){
            VectorMessage msg = iter.next();
            if(inOrder(msg)){
                updateVector(msg);
                callback.accept(msg.getContent());
                iter.remove();
                updateQueue(callback);
                return;
            }
        }
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
        logVector.write(vector);
        VectorMessage vm = new VectorMessage<>(id, vector, content);
        return s.encode(vm);
    }
/*
    public CompletableFuture<Void> sendAsyncToCluster(List<Address> servers, String type, T content) {
        VectorMessage<T> msg = createMsg(content);
        for (Address a : servers)
            mms.sendAsync(a, type, s.encode(msg));
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAsync( Address a, String type, T content){
        VectorMessage msg = createMsg(content);
        System.out.println(msg.toString());
        return mms.sendAsync(a, type, s.encode(msg));
    }

    public CompletableFuture<Void> sendAndReceive(T content, String type, Address a, ExecutorService e, Consumer<Object> cvm){
        VectorMessage msg = createMsg(content);
        return mms.sendAndReceive(a, type, s.encode(msg), e)
                .thenAccept((b) -> read(b, cvm));
    }

*/
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
