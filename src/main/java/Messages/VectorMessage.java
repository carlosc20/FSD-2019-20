package Messages;

import java.util.List;
import java.util.ArrayList;

public class VectorMessage extends Message {
    List<Integer> v;

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

    public VectorMessage(int id, String msg, int numParticipants, List<Integer> v){
        super(id, msg);
        this.v = v;
    }

    public VectorMessage(VectorMessage m){
        super(m.getId(), m.getMsg());
        this.v = m.getVector();
    }

    public void fillFromByteArray (byte[] b){
        String msg = new String(b);
        String[] vars = msg.split("&");
        this.setId(Integer.parseInt(vars[0]));
        this.setMsg(vars[1]);
        String[] counters = vars[2].split(",");
        for(String c : counters)
            v.add(Integer.parseInt(c));
    }

    public byte[] toByteArray(){
        String res = "";
        res += this.getId() + "&";
        res += this.getMsg() + "&";
        for(Integer i : v)
            res+= Integer.toString(i) + ",";
        return res.getBytes();
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
    public String toString() {
        String vec="";
        for(Integer i : v){
            vec += Integer.toString(i) + '/';
        }
        return "VectorMessage{ " +
                super.toString() +
                " v= " + vec +
                '}';
    }
}
