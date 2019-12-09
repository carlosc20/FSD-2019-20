package Middleware.TwoPhaseCommit;

import Middleware.CausalOrdering.CausalOrderHandler;
import io.atomix.utils.net.Address;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class Participant {
    private int id;
    private Address manager;
    private CausalOrderHandler<TransactionMessage> coh;
    private ExecutorService e;
    private Map<Integer, Object> objectsToSend;
    private int numObjects;


    public Participant(int id, List<Address> servers, Address manager, Address myAddr){
        this.id = id;
        this.manager = manager;
        this.coh = new CausalOrderHandler<>(id,servers,myAddr);
        this.e = Executors.newFixedThreadPool(1);
        this.objectsToSend = new HashMap<>();
        this.numObjects = 0;

    }

    public void startProtocol() {
        System.out.println(id + ": Starting participant protocol");
        //TODO resolver o martelanço
        coh.registerHandlerMartelado("forController", e, parseAndExecute);
        coh.registerHandler("participant", e, parseAndExecute);
        //coh.registerHandler("controller", e, (a,b)->{});
    }
    //TODO ? em vez de ter tantos typos de msg ter mais registerHandlers?
    //b -> begin (pedido de transação)
    //t -> begin (transação)
    //p -> prepared
    //e -> execute
    //c -> commit
    //a -> abort
    //Maneira muito simples para testar
    private BiConsumer<Object, Address> parseAndExecute = (o,a) -> {
        TransactionMessage tm = (TransactionMessage) o;
        switch (tm.getType()) {
            case 'b':
                int id = tm.getMessageId();
                //TODO verificar se existe? pode ter recebido logo após um reboot..
                Object toSend = objectsToSend.get(id);
                //tm já tem número de transação.
                execute(tm, toSend)
                        .thenAccept((n)->commit(tm));
                break;
            case 'e':
                System.out.println(this.id + ": Executing");
                //TODO vai colocar em estado temporário numa estrutura auxiliar. Dar logg da estrutura??
                System.out.println("Server " + this.id +" received " + tm.getContent());
                break;
            case 'p':
                tm.setType('p');
                coh.sendAsyncMartelado(tm, "controller", manager);
                //coh.sendAsyncToCluster(tm, "participant");
                break;
            case 'c':
                //TODO logg
                System.out.println("Server " + this.id +" Commited");
                break;
            case 'a':
                //TODO logg
                System.out.println("Server " + this.id +" Aborted");
                break;
        }
    };

    public CompletableFuture<Void> begin(Object toSend){
        System.out.println(id + ": Beginning");
        TransactionMessage tm = new TransactionMessage();
        numObjects++;
        objectsToSend.put(numObjects, toSend);
        //TODO dar logg de início de pedido de transação
        tm.setMessageId(numObjects);
        return coh.sendAsyncMartelado(tm,"controller", manager);
        //return coh.sendAsyncToCluster(tm, "controller");
    }

    public CompletableFuture<Void> execute(TransactionMessage tm, Object toSend){
        System.out.println(this.id + ": Starting execution");
        tm.setContent(toSend);
        tm.setType('e');
        return coh.sendAsyncToCluster(tm, "participant").
                thenAccept((n)-> objectsToSend.remove(id)); //TODO Logg de execução temporária iniciada

    }

    private CompletableFuture<Void> commit(TransactionMessage tm){
        System.out.println(id + ": Commiting");
        tm.setContent(null);
        tm.setType('t');
        //TODO dar logg de inicio de uma transação
        return coh.sendAsyncMartelado(tm, "controller", manager);
        //return coh.sendAsyncToCluster(tm, "participant");
    }

    public static void main(String[] args) throws InterruptedException {
        Address manager = Address.from("localhost",12345);
        Address ap1 = Address.from("localhost",12346);
        Address ap2 = Address.from("localhost",12347);
        ArrayList<Address> addrs = new ArrayList<>();
        addrs.add(ap1); addrs.add(ap2);

        Manager m = new Manager(0, addrs, manager);
        Participant p1 = new Participant(1, addrs, manager, ap1);
        Participant p2 = new Participant(2, addrs, manager, ap2);

        m.startProtocol();
        p2.startProtocol();
        p1.startProtocol();
        p1.begin("Olá colega");
    }
}
