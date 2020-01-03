package Middleware.Recovery;

public class MessageRecovery {

    private int id; //SENDER id
    private int total;  //total is equal to the number of messages server did not receive.

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
