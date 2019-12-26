package Middleware.TwoPhaseCommit;

public class TransactionMessage<V>  {
    private int transactionId;
    private char type;
    private String operationName;
    private V content;

    public TransactionMessage(int transactionId, String operationName, V content){
        this.transactionId = transactionId;
        this.type = 'b';
        this.operationName = operationName;
        this.content = content;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public String getOperationName() {
        return operationName;
    }

    public V getContent() {
        return content;
    }

    public void setContent(V content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "TransactionMessage{" +
                " tId= " + this.transactionId +
                " type= " + this.type +
                '}';
    }
}
