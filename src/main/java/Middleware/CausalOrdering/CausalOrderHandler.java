package Middleware.CausalOrdering;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.function.Consumer;

public class CausalOrderHandler<T extends VectorOrdering> {
    private int id;
    private int numPeers; // remover
    private List<Integer> counters; // counter
    private Queue<T> waitingMsg;
    private ManagedMessagingService mms;
    private ArrayList<Address> servers;

    //para testes
    public CausalOrderHandler(int id, int numPeers){
        this.id = id;
        this.numPeers = numPeers;
        this.counters = new ArrayList<>();
        for(int i = 0; i<numPeers; i++)
            this.counters.add(0);
        this.waitingMsg = new LinkedList<>();
    }

    public CausalOrderHandler(int id, ManagedMessagingService mms, List<Address> servers){
        this.id = id;
        this.counters = new ArrayList<>();
        this.mms = mms;
        int numPeers = servers.size();
        this.servers = new ArrayList<>(numPeers -1);
        for(int i = 0; i<numPeers; i++){
            this.counters.add(0);
            //Hardcoded...servidores tem de ter id's correspondentes a indices -> 0,1..N
            if(i==id)
                continue;
            this.servers.add(servers.get(i));
        }
        this.waitingMsg = new LinkedList<>();
    }

    //Callback passou a ser do método read e não da classe
    public void read(T msg, Consumer<T> callback){
        if(inOrder(msg)){
            setCausality(msg);
            callback.accept(msg);
            updateQueue(callback);
           //System.out.println("ordem correta");
        }
        else{
           //System.out.println("ordem errada");
            waitingMsg.add(msg);
        }
    }

    private void setCausality(T msg){
        counters.set(msg.getId(), msg.getElement(msg.getId()));
    }

    private void updateQueue(Consumer<T> callback){
        Iterator<T> iter = waitingMsg.iterator();
        //TODO: otimizar
        boolean found = false;
        while (iter.hasNext() && !found){
            T msg = iter.next();
            if(inOrder(msg)){
                setCausality(msg);
                callback.accept(msg);
                iter.remove();
                updateQueue(callback);
                found = true;
            }
        }
    }

    private boolean inOrder(T msg){
        List<Integer> v = msg.getVector();
        boolean condition = true;
        for(int i=0; condition && i < v.size(); i++){
            if(i != msg.getId()){
                if(v.get(i) > counters.get(i)){
                    condition = false;
                    System.out.println("1ª");
                }
            }
            else
                if(counters.get(i) + 1 != v.get(i)){
                    condition = false;
                }
            }
        return condition;
    }

//para os testes ainda funcionarem -> deprecated
    public void sendToCluster(String msg) {
        counters.set(id, counters.get(id) + 1);
        VectorMessage m = new VectorMessage(id,counters);
        for (Address a : servers)
            mms.sendAsync(a, "vectorMessage", VectorMessage.serializer.encode(m));
    }

    public void sendToCluster(T semiMsg, Serializer s, String destiny) {
        updateMsgAndCounter(semiMsg);
        for (Address a : servers)
            mms.sendAsync(a, destiny, s.encode(semiMsg));
    }

    public void sendAnswer(T semiMsg, Serializer s, String destiny, Address a){
        updateMsgAndCounter(semiMsg);
        mms.sendAsync(a, destiny, s.encode(semiMsg));
    }

    private void updateMsgAndCounter(T semiMsg){
        counters.set(id, counters.get(id) + 1);
        semiMsg.setId(id);
        semiMsg.setVector(counters);
    }


    public static void main(String[] args) throws InterruptedException {
        /*
        Consumer<VectorMessage> cvm = (msg)-> System.out.println(msg.getMsg());
        CausalOrderHandler coh = new CausalOrderHandler(0,2);
        //coh.setCallback(cvm);
        Random r = new Random();
        int low = 0;
        int high = 6;

        VectorMessage vm1 = new VectorMessage(1,"olá", 2);
        VectorMessage vm2 = new VectorMessage(1,"adeus", 2);
        VectorMessage vm3 = new VectorMessage(1,"afinal olá", 2);

        vm1.setVectorIndex(1,1);
        vm2.setVectorIndex(1,2);
        vm3.setVectorIndex(1,3);

        coh.read(vm3);
        Thread.sleep(2000);
        coh.read(vm2);
        Thread.sleep(2000);
        coh.read(vm1);

    //Bloco de código concorrente! Métodos desta classe não suportam concorrência. Meter synchronized para testar
        for(int i=1; i<=100; i++){
            VectorMessage vm = new VectorMessage(1,i + ": oi", 2);
            vm.setVectorIndex(1,i);
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
