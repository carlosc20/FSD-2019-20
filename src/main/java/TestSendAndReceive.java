import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestSendAndReceive {
    private int port;
    private ManagedMessagingService mms;
    private ExecutorService e;

    public TestSendAndReceive(int port){
        this.port = port;
        this.mms = new NettyMessagingService("irr", Address.from("localhost",port), new MessagingConfig());
        mms.start();
        this.e = Executors.newFixedThreadPool(1);
    }

    public void receive() {
        mms.registerHandler("a", (c, d) -> {
            String msg = "olá " + c.address() + " eu sou o " + this.port + " " + new String(d);
            return msg.getBytes();
        }, e);
    }

    public void send(String msg, Address b) throws ExecutionException, InterruptedException {
        byte[] by = msg.getBytes();
        mms.sendAndReceive(b,"a", by, e).thenAccept((toPrint) -> System.out.println(new String(toPrint)));
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TestSendAndReceive tsr1 = new TestSendAndReceive(12345);
        TestSendAndReceive tsr2 = new TestSendAndReceive(12346);
        tsr2.receive();
        tsr1.send("o", Address.from(12346));
    }
}
