package Middleware.CausalOrdering;

import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.ArrayList;

public class VectorMessage<T> implements VectorOrdering, Message {

    private int id;
    private List<Integer> v;
    private T content;

    public VectorMessage(){
        this.id = -1;
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, List<Integer> v, T content){
        this.id = id;
        this.v = new ArrayList<>();
        this.content = content;
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

    @Override
    public void setIndex(int index, int value) {
        this.v.set(index,value);
    }

    public void setVector(List<Integer> v) {
        this.v = v;
    }

    public List<Integer> getVector(){
        return this.v;
    }

    @Override
    public int getIndex(int index) {
        return v.get(index);
    }

    @Override
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

    @Override
    public Serializer getSerializer() {
        return new SerializerBuilder()
                .addType(VectorMessage.class)
                .addType(List.class)
                .build();
    }
}
