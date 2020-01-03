package Middleware.CausalOrder;

import Middleware.Logging.Logger;
import Middleware.Recovery.MessageRecovery;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.function.Consumer;

public class CausalOrderHandler {

    private int sequenceNumber = 0;

    private int id;
    private List<Integer> vector;
    private Queue<VectorMessage> msgQueue;

    private Serializer s;
    private NeighboursRecoveryAssistant nra;


    public CausalOrderHandler(int id, int clusterSize, Serializer s, Logger log){
        this.id = id;
        this.vector = new ArrayList<>();
        for(int i = 0; i<clusterSize; i++){
            this.vector.add(0);
        }
        this.msgQueue = new LinkedList<>();
        this.s = s;
        this.nra = new NeighboursRecoveryAssistant(id, vector, log);
    }


    public MessageRecovery getMissingOperationMessage(){
        return nra.getMissingOperationMessage();
    }

    public VectorMessage getVectorMessage(int messageId){
        return nra.getVectorMessage(messageId);
    }

    public List<Integer> getVector() {
        return vector;
    }

    public byte[] createMsg(Object content) {
        vector.set(id, vector.get(id) + 1); // incrementa vetor local
        VectorMessage vm = new VectorMessage(id, vector, content);
        nra.saveUnackedOperation(vm);
        nra.logOrderedOperation(vm);
        System.out.println("coh.createMsg -> creating msg " + vm.toString() + "to send");
        return s.encode(vm);
    }

    public void read(byte[] b, Consumer<Object> callback){
        read(0, b, callback);
    }

    public void recoveryRead(byte[] b, Consumer<Object> callback){
        read(1, b, callback);
    }

    public void resendMessagesRead(byte[] b, Consumer<Object> callback){
        read(3, b, callback);
    }

    // type 0 -> normal, 1 -> logs
    private void read(int type, byte[] b, Consumer<Object> callback){
        sequenceNumber++;
        VectorMessage msg = s.decode(b);
        System.out.println("coh.read"+type+ " -> Received a msg "+ msg.toString() + " sequenceNumber = " + sequenceNumber);
        System.out.println("coh.read"+type+ " -> from server " + msg.getId() + " and I have " + vector.get(msg.getId())+ " as is clock");

        if(type == 0) // normal
            nra.logOrderedOperation(msg);
        else if(msg.getId() == id) { // logs && msg que foram enviadas por este servidor
            nra.saveUnackedOperation(msg);
        }
        else if(type == 3){
            for(VectorMessage vm : msgQueue)
                if(msg.equals(vm)) return;
            nra.logOrderedOperation(msg);
        }
        // vê se está ordenada, se não vai para queue
        if(inOrder(msg)){
            System.out.println("coh.read"+type+ " -> inOrder");
            updateVector(msg);
            if(msg.getId() != id)
                nra.updateClocks(msg);
            callback.accept(msg.getContent());
            updateQueue(type, callback);
        }
        else{
            System.out.println("coh.read"+type+ " -> outOfOrder");
            msgQueue.add(msg);
        }
    }

    /*  verifica se alguma das mensagens na queue está ordenada, se sim executa a callback
        atualiza estruturas e volta a verificar a queue agora com a mensagem removida
     */
    private void updateQueue(int type, Consumer<Object> callback){
        Iterator<VectorMessage> iter = msgQueue.iterator();
        while (iter.hasNext()){
            VectorMessage msg = iter.next();
            if(inOrder(msg)){
                if(type == 0)
                    nra.logOrderedOperation(msg);
                updateVector(msg);
                if(msg.getId() != id)
                    nra.updateClocks(msg);
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

    // verifica se a mensagem está ordenada
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



}
