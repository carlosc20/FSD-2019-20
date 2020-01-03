package Middleware.Recovery;

public class MessageRecovery {
    int id; //SENDER id
    //total is equal to the number of messages server did not receive.
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
