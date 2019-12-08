package Middleware.CausalOrdering;

import io.atomix.utils.net.Address;

import java.util.List;
import java.util.ArrayList;

public class VectorMessage<T> implements VectorOrdering, Message {

    private int id;
    private Address sender;
    private List<Integer> v;
    private T content;

    public VectorMessage(){
        this.id = -1;
        this.sender = null;
        this.v = new ArrayList<>();
    }

    public VectorMessage(int id, List<Integer> v, T content, Address sender){
        this.sender = sender;
        this.id = id;
        this.v = v;
        this.content = content;
    }


    public VectorMessage(int id, List<Integer> v, T content){
        this.id = id;
        this.v = v;
        this.content = content;
    }

    public VectorMessage(int id, List<Integer> v){
        this.id = id;
        this.v = v;
    }

    public T getContent(){
        return content;
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

    public Address getSender() {
        return sender;
    }

    public void setSender(Address sender) {
        this.sender = sender;
    }

    @Override
    public int getIndex(int index) {
        return v.get(index);
    }

    @Override
    public String toString() {
        String content = this.content == null ? "null" : this.content.toString();
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(Integer.toString(i)).append('/');
        }
        return "VectorMessage{ " +
                " serverId= " + this.id +
                " v= " + strb.toString() +
                " content= " + content +
                '}';
    }
}
