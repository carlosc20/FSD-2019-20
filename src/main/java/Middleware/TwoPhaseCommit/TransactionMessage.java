package Middleware.TwoPhaseCommit;

public class TransactionMessage  {
    private int transactionId;
    private char response;
    private Object content;

    public TransactionMessage(){
        this.transactionId = -1;
        this.response = 'r';
        this.content = null;
    }

    public TransactionMessage(int transactionId, char response){
        this.transactionId = transactionId;
        this.response = response;
        this.content = null;
    }

    public TransactionMessage(int transactionId, char response, Object content){
        this.transactionId = transactionId;
        this.response = response;
        this.content = content;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public char getResponse() {
        return response;
    }

    public void setResponse(char response) {
        this.response = response;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "TransactionMessage{" +  super.toString() +
                "ident=" +
                ", response=" + response +
                '}';
    }
}
