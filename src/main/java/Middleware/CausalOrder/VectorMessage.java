package Middleware.CausalOrder;

import java.util.List;

public class VectorMessage {

    private int id;
    private List<Integer> v;
    private Object content;
    private String operation; //para a recovery

    public VectorMessage(int id, List<Integer> v, Object content, String operation){
        this.id = id;
        this.v = v;
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
        return this.v;
    }

    public String getOperation() {
        return operation;
    }

    public int getIndex(int index) {
        return v.get(index);
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
