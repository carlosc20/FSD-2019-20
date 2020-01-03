package Middleware.CausalOrder;

import java.util.ArrayList;
import java.util.List;

public class VectorMessage {

    private int id; // id de quem envia
    private List<Integer> vector;
    private Object content;

    public VectorMessage(int id, List<Integer> v, Object content){
        this.id = id;
        this.vector = new ArrayList<>(v);
        this.content = content;
    }

    public Object getContent(){
        return content;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Integer> getVector(){
        return this.vector;
    }


    public int getIndex(int index) {
        return vector.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorMessage that = (VectorMessage) o;
        return id == that.id &&
                vector.equals(that.vector);
    }

    @Override
    public String toString() {
        String content = this.content == null ? "null" : this.content.toString();
        StringBuilder strb = new StringBuilder();
        for(Integer i : vector){
            strb.append(i).append('/');
        }
        return "VectorMessage{ " +
                " serverId= " + this.id +
                " vector= " + strb.toString() +
                " content= " + content +
                '}';
    }
}
