package Middleware.TwoPhaseCommit;

public class TransactionMessage  {
    private int messageId;
    private int transactionId;
    private char type;
    private Object content;

    public TransactionMessage(){
        this.messageId = -1;
        this.transactionId = -1;
        this.type = 'b';
        this.content = null;
    }

    public TransactionMessage(int messageId, int transactionId, char response){
        this.messageId = messageId;
        this.transactionId = transactionId;
        this.type = response;
        this.content = null;
    }

    public TransactionMessage(int messageId, int transactionId, char response, Object content){
        this.messageId = messageId;
        this.transactionId = transactionId;
        this.type = response;
        this.content = content;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
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

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    @Override
    public String toString() {
        String content = this.content == null ? "null" : this.content.toString();
        return "TransactionMessage{" +
                " mId= " + this.messageId +
                " tId= " + this.transactionId +
                " type= " + this.type +
                " content= " + content +
                '}';
    }
}
