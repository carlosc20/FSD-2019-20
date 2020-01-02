package Middleware.CausalOrder;

import java.util.List;

public class VectorMessage {

    private int id; // id do sender
    private List<Integer> vector;
    private Object content;
    private String operation; //para a recovery

    public VectorMessage(int id, List<Integer> v, Object content, String operation){
        this.id = id;
        this.vector = v;
        this.content = content;
        this.operation = operation;
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

    public String getOperation() {
        return operation;
    }

    public int getIndex(int index) {
        return vector.get(index);
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
