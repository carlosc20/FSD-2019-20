package Middleware.CausalOrdering;

import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.List;
import java.util.ArrayList;

public class VectorMessage implements VectorOrdering {
    private int id;
    private List<Integer> v;

    public static Serializer serializer = new SerializerBuilder()
            .addType(VectorMessage.class)
            .addType(List.class)
            .build();

    public VectorMessage(){
        this.id = -1;
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, String msg){
        this.id = id;
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, int numParticipants){
        this.id = id;
        this.v = new ArrayList<>();
        for(int i = 0; i<numParticipants; i++)
            this.v.add(0);
    }

    public VectorMessage(int id, List<Integer> v){
        this.id = id;
        this.v = v;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int id) {
        this.id = id;
    }

    public void setV(List<Integer> v) {
        this.v = v;
    }

    public void setVector(List<Integer> v) {
        this.v = v;
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
