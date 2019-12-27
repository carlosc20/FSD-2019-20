package Middleware.Marshalling;

public class MessageRecovery {
    int id; //SENDER id
    int savepoint; //receiver clock in sender coh

    public MessageRecovery(int id, int savepoint){
        this.id = id;
        this.savepoint = savepoint;
    }

    public int getId() {
        return id;
    }

    public int getSavepoint() {
        return savepoint;
    }

    @Override
    public String toString() {
        return "MessageRecovery{" +
                "id=" + id +
                ", clock=" + savepoint +
                '}';
    }
}
