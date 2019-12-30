import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class TestSendAndReceive {
    private int port;
    private ManagedMessagingService mms;
    private ExecutorService e;
    int i = 0;

    public TestSendAndReceive(int port){
        this.port = port;
        this.mms = new NettyMessagingService("irr", Address.from("localhost",port), new MessagingConfig());
        mms.start();
        this.e = Executors.newFixedThreadPool(1);
    }

    public void receive() {
        mms.registerHandler("a", (c, d) -> {
            String msg = "olá " + c.address() + " eu sou o " + this.port + " " + new String(d);
            if(i == 0){
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }}
            i++;
            return msg.getBytes();
        }, e);
    }

    public CompletableFuture<byte[]> send2(String msg, Address b){
        return send1(msg,b).thenCompose(msg2 -> {
            if(msg2 == null)
                return send2(msg,b);
            return CompletableFuture.completedFuture(msg2);
        });
    }

    public CompletableFuture<byte[]> send1(String msg, Address b){
        byte[] by = msg.getBytes();
        return mms.sendAndReceive(b,"a", by, Duration.ofSeconds(3), e)
                .handle((m,t) -> {
                    if(t!=null){
                        System.out.println("timeout");
                        return null;
                    }
                    else
                        return m;
                });
    }

    public void send(String msg, Address b, Consumer<Object> callback){
        byte[] by = msg.getBytes();
        mms.sendAndReceive(b,"a", by, Duration.ofSeconds(3), e).whenComplete((m, throwable) ->{
            if(throwable!=null) {
                System.out.println("resending");
                send(msg, b, callback);
            }
            else
                callback.accept(m);
        });
    }

    public static void main(String[] args) throws InterruptedException {
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        TestSendAndReceive tsr = new TestSendAndReceive(10000+id);
        if(id == 0){
            tsr.receive();
        }
        else {
            tsr.receive();
            tsr.send2("o", Address.from(10000)).thenAccept(x -> {
                        byte[] str = (byte[]) x;
                        String ptr = new String(str);
                        System.out.println(ptr);
                    });
            System.out.println("oijfoimfapenf");
        }
    }
}
