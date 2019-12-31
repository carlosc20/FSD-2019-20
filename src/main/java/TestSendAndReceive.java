import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.time.Duration;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class TestSendAndReceive {
    private int port;
    private ManagedMessagingService mms;
    private ExecutorService e;
    private ScheduledExecutorService ses;
    int i = 0;

    public TestSendAndReceive(int port){
        this.port = port;
        this.mms = new NettyMessagingService("irr", Address.from("localhost",port), new MessagingConfig());
        mms.start();
        this.e = Executors.newFixedThreadPool(1);
        this.ses = Executors.newScheduledThreadPool(1);
    }

    public void receive() {
        mms.registerHandler("a", (c, d) -> {
            String msg = "olá " + c.address() + " eu sou o " + this.port + " " + new String(d);
            if(i == 0){
            try {
                Thread.sleep(7000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }}
            i++;
            return msg.getBytes();
        }, e);
    }

    public CompletableFuture<byte[]> send(String msg, Address b){
        byte[] by = msg.getBytes();
        CompletableFuture<byte[]> cf = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = ses.scheduleAtFixedRate(()->
            mms.sendAndReceive(b,"a", by, Duration.ofSeconds(1), e)
                    .whenComplete((m,t) -> {
                        if(t!=null){
                            System.out.println("timeout");
                        }
                        else{
                            System.out.println("completing future");
                            cf.complete(m);
                        }}), 0, 4, TimeUnit.SECONDS);
        cf.whenComplete((m,t) -> scheduledFuture.cancel(true));
        return cf;
    }


    public CompletableFuture<byte[]> send2(String msg, Address b){
        Runnable task = () -> send2(msg, b);
        ses.schedule(task, 4, TimeUnit.SECONDS);
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

    public static void main(String[] args) throws InterruptedException {
        int id = Integer.parseInt(new Scanner(System.in).nextLine());
        TestSendAndReceive tsr = new TestSendAndReceive(10000+id);
        if(id == 0){
            tsr.receive();
        }
        else {
            tsr.receive();
            tsr.send("o", Address.from(10000)).thenAccept(x -> {
                        byte[] str = (byte[]) x;
                        String ptr = new String(str);
                        System.out.println(ptr);
                    });
        }
    }
}
