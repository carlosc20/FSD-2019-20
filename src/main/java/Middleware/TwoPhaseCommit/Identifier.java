package Middleware.TwoPhaseCommit;

import java.util.Objects;

public class Identifier {
    private int serverId;
    private int transactionId;

    public Identifier(int serverId, int transactionId){
        this.serverId = serverId;
        this.transactionId = transactionId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, transactionId);
    }
}
