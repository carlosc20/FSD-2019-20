package Middleware.DistributedStructures;

public class OperationMessage<V>{
    private int transactionId;
    private V content;
    private String operationName;

    public OperationMessage(V content, String operationName){
        this.transactionId = -1;
        this.content = content;
        this.operationName = operationName;
    }

    public OperationMessage(int transactionId, V content, String operationName){
        this.transactionId = transactionId;
        this.content = content;
        this.operationName = operationName;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public V getContent() {
        return content;
    }

    public String getOperationName() {
        return operationName;
    }

    @Override
    public String toString() {
        return "OperationMessage{" +
                "transactionId=" + transactionId +
                ", content=" + content.toString() +
                ", operationName='" + operationName + '\'' +
                '}';
    }
}
