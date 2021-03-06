package Middleware.TwoPhaseCommit;

public class TransactionMessage<V>  {
    private int requestId;
    private Identifier transactionId;
    private char type;
    private int phase;
    private V content;

    public TransactionMessage(int requestId, V content){
        this.requestId = requestId;
        this.transactionId = null;
        this.type = 'b';
        this.phase = 0;
        this.content = content;
    }

    public Identifier getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Identifier transactionId) {
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

    public void setFinished(){this.type = 'f';}

    public boolean notStarted(){
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

    public boolean isFinished() {return type == 'f';}

    public V getContent() {
        return content;
    }

    public int getRequestId() {
        return requestId;
    }

    public void setContent(V content) {
        this.content = content;
    }

    public boolean isSecondPhase(){return phase == 2;}

    public boolean isFirstPhase(){return phase == 1;}


    public void setPhase(int phase) {
        this.phase = phase;
    }



    @Override
    public String toString() {
        String content;
            if(transactionId == null)
                content = "null";
            else
                content = transactionId.toString();
        return "TransactionMessage{" +
                "senderId=" + requestId +
                ", transactionId=" + content +
                ", content= " + content.toString() +
                ", type=" + type +
                ", phase=" + phase +
                '}';
    }
}
