package Middleware.TwoPhaseCommit;
import io.atomix.utils.net.Address;

import java.util.Objects;

public class Identifier {
    private int serverId;
    private int id;

    public Identifier(int serverId, int id){
        this.serverId = serverId;
        this.id = id;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public void setTransactionId(int id) {
        this.id = id;
    }

    public int getServerAddress() {
        return serverId;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Identifier that = (Identifier) o;
        return serverId == that.serverId &&
                id == that.id;
    }

    @Override
    public String toString() {
        return "Identifier{" +
                "serverId" + serverId +
                ", transactionId=" + id+
                '}';
    }
}