package Middleware;

public abstract class Message {

    private int id;
    private String msg;

    public Message(){
        id = 0;
        msg = "";
    }

    public Message(int id, String msg){
        this.id = id;
        this.msg = msg;
    }

    public int getId() {
        return id;
    }

    public String getMsg() {
        return msg;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "Message{ " +
                "id=" + id +
                ", msg='" + msg + '\'' +
                '}';
    }
}
