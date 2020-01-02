package Middleware.CausalOrder;

import java.util.List;
import java.util.Objects;

public class VectorMessage {

    private int id;
    private List<Integer> v;
    private Object content;

    public VectorMessage(int id, List<Integer> v, Object content){
        this.id = id;
        this.v = v;
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
        return this.v;
    }


    public int getIndex(int index) {
        return v.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorMessage that = (VectorMessage) o;
        return id == that.id &&
                v.equals(that.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, v);
    }

    @Override
    public String toString() {
        String content = this.content == null ? "null" : this.content.toString();
        StringBuilder strb = new StringBuilder();
        for(Integer i : v){
            strb.append(i).append('/');
        }
        return "VectorMessage{ " +
                " serverId= " + this.id +
                " v= " + strb.toString() +
                " content= " + content +
                '}';
    }
}
