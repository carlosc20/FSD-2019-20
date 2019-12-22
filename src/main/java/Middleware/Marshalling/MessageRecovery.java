package Middleware.Marshalling;

public class MessageRecovery {
    int id; //SENDER id
    int clock; //receiver clock in sender coh

    public MessageRecovery(int id, int clock){
        this.id = id;
        this.clock = clock;
    }

    public int getId() {
        return id;
    }

    public int getClock() {
        return clock;
    }

    @Override
    public String toString() {
        return "MessageRecovery{" +
                "id=" + id +
                ", clock=" + clock +
                '}';
    }
}
