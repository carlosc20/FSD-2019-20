package Middleware.Recovery;

public class MessageRecovery {
    int id; //SENDER id
    //if reply total is number of messages server did not receive.
    //if request total is last clock registered from target
    int total;

    public MessageRecovery(int id, int total){
        this.id = id;
        this.total = total;
    }

    public int getId() {
        return id;
    }


    public int getTotal() {
        return total;
    }

    @Override
    public String toString() {
        return "MessageRecovery{" +
                "id=" + id +
                "total=" + total +
                '}';
    }
}
