package Middleware.CausalOrdering;

import Middleware.Message;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.List;
import java.util.ArrayList;

public class VectorMessage extends Message implements VectorOrdering {

    private List<Integer> v;

    public static Serializer serializer = new SerializerBuilder()
            .addType(VectorMessage.class)
            .addType(List.class)
            .build();

    public VectorMessage(){
        super();
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, String msg){
        super(id, msg);
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, String msg, int numParticipants){
        super(id, msg);
        this.v = new ArrayList<>();
        for(int i = 0; i<numParticipants; i++)
            this.v.add(0);
    }

    public VectorMessage(int id, String msg, List<Integer> v){
        super(id, msg);
        this.v = v;
    }

    public VectorMessage(VectorMessage m){
        super(m.getId(), m.getMsg());
        this.v = m.getVector();
    }

    public void setVector(List<Integer> v) {
        this.v = v;
    }

    public void addToVector(int c){
        this.v.add(c);
    }

    public void setVectorIndex(int index, int value){
        this.v.set(index,value);
    }

    public List<Integer> getVector(){
        return this.v;
    }
    public int getElement(int index){
        return v.get(index);
    }

    @Override
    //TODO usar o stringbuilder
    public String toString() {
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(Integer.toString(i)).append('/');
        }
        return "VectorMessage{ " +
                super.toString() +
                " v= " + strb.toString() +
                '}';
    }
}
