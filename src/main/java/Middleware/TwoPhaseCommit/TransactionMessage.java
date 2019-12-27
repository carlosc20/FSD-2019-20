package Middleware.TwoPhaseCommit;

public class TransactionMessage<V>  {
    private int transactionId;
    private char type;
    private V content;

    public TransactionMessage(V content){
        this.transactionId = -1;
        this.type = 'b';
        this.content = content;
    }

    public TransactionMessage(int transactionId, V content){
        this.transactionId = transactionId;
        this.type = 'b';
        this.content = content;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public void setCommited(){
        this.type = 'c';
    }

    public void setAborted(){
        this.type = 'a';
    }

    public void setPrepared(){
        this.type = 'p';
    }

    public boolean notVoted(){
        return type == 'b';
    }

    public boolean isAborted(){
        return type == 'a';
    }

    public  boolean isPrepared(){
        return  type == 'p';
    }

    public boolean isCommited(){
        return type == 'c';
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
