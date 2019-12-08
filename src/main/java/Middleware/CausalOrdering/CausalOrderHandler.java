package Middleware.CausalOrdering;

import Middleware.TwoPhaseCommit.TransactionMessage;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;


//TODO meter endereços nas vector messages.

public class CausalOrderHandler<T> {

    private int id;
    private List<Integer> vector;
    private Queue<VectorMessage> msgQueue;
    private ManagedMessagingService mms;
    private ArrayList<Address> servers;
    private Serializer s;

    public CausalOrderHandler(int id, List<Address> servers, Address myAddr){
        this.id = id;
        this.vector = new ArrayList<>();
        this.mms =  new NettyMessagingService("irr", myAddr, new MessagingConfig());
        mms.start();
        this.servers = new ArrayList<>();
        for(int i = 0; i<servers.size(); i++){
            this.vector.add(0);
            // servidores tem de ter id's correspondentes a indices -> 0,1..N
            if(i+1==id) continue;
            this.servers.add(servers.get(i));
        }
        this.msgQueue = new LinkedList<>();
        this.s = new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .addType(TransactionMessage.class)
                .addType(Object.class)
                .addType(Address.class)
                .build();
    }

    public void registerHandler(String type, ExecutorService e, BiConsumer<Object, Address> bf){
        mms.registerHandler(type, (a, b) -> {
            VectorMessage msg = s.decode(b);
            System.out.println(id + ": Received a msg "+ msg.toString());
            System.out.println(id + ": from: " + msg.getId() + " i have: " + vector.get(msg.getId()));
            if(inOrder(msg)){
                System.out.println(id + ": inOrder");
                updateVector(msg);
                bf.accept(msg.getContent(),a);
                updateQueue(bf);
            }
            else{
                System.out.println(id + ": outOfOrder");
                msg.setSender(a);
                msgQueue.add(msg);
            }
        }, e);
    }

    public void registerHandlerMartelado(String type, ExecutorService e, BiConsumer<Object, Address> bf){
        mms.registerHandler(type, (a, b) -> {
            VectorMessage msg = s.decode(b);
            System.out.println(id + ": Received a msg "+ msg.toString());
            bf.accept(msg.getContent(),a);
        }, e);
    }

    public CompletableFuture<Void> sendAsyncMartelado(T content, String type, Address a){
        VectorMessage msg = new VectorMessage<>(id, vector, content);
        System.out.println(msg.toString());
        return mms.sendAsync(a, type, s.encode(msg));
    }

    private void updateVector(VectorMessage msg){
        int id = msg.getId();
        vector.set(id, msg.getIndex(id));
    }

    private void updateQueue(BiConsumer<Object, Address> callback){
        Iterator<VectorMessage> iter = msgQueue.iterator();
        while (iter.hasNext()){
            VectorMessage msg = iter.next();
            if(inOrder(msg)){
                updateVector(msg);
                callback.accept(msg.getContent(), msg.getSender());
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

    private VectorMessage<T> createMsg(T content) {
        vector.set(id, vector.get(id) + 1); // incrementa vetor local
        return new VectorMessage<>(id, vector, content);
    }

    // TESTES
    public CausalOrderHandler(int id, int numPeers){
        this.id = id;
        this.vector = new ArrayList<>();
        for(int i = 0; i<numPeers; i++)
            this.vector.add(0);
        this.msgQueue = new LinkedList<>();
    }

    public void sendToCluster(String msg) {
        vector.set(id, vector.get(id) + 1);
        VectorMessage m = new VectorMessage(id, vector);
        for (Address a : servers)
            mms.sendAsync(a, "vectorMessage", s.encode(m));
    }
    //TODO não sei se serve de algo ter CF aqui
    public CompletableFuture<Void> sendAsyncToCluster(T content, String type) {
        VectorMessage<T> msg = createMsg(content);
        for (Address a : servers)
            mms.sendAsync(a, type, s.encode(msg));
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> sendAsync(T content, String type, Address a){
        VectorMessage msg = createMsg(content);
        System.out.println(msg.toString());
        return mms.sendAsync(a, type, s.encode(msg));
    }
/*
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
