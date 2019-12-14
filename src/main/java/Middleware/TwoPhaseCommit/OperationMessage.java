package Middleware.TwoPhaseCommit;

public class OperationMessage{
    private int transactionId;
    private Object content;

    public OperationMessage(int transactionId, Object content){
        this.transactionId = transactionId;
        this.content = content;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public Object getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "OperationMessage{" +
                "transactionId=" + transactionId +
                ", content=" + content +
                '}';
    }
}
