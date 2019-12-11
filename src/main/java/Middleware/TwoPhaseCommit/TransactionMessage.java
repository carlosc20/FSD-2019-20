package Middleware.TwoPhaseCommit;

public class TransactionMessage  {
    private Identifier transactionId;
    private char type;
    private Object content;

    public TransactionMessage(){
        this.transactionId = new Identifier();
        this.type = 'b';
        this.content = null;
    }

    public TransactionMessage(Identifier transactionId, char type){
        this.transactionId = transactionId;
        this.type = type;
        this.content = null;
    }

    public TransactionMessage(Identifier transactionId, char type, Object content){
        this.transactionId = transactionId;
        this.type = type;
        this.content = content;
    }

    public Identifier getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Identifier transactionId) {
        this.transactionId = transactionId;
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    @Override
    public String toString() {
        String content = this.content == null ? "null" : this.content.toString();
        return "TransactionMessage{" +
                " tId= " + this.transactionId +
                " type= " + this.type +
                " content= " + content +
                '}';
    }
}
