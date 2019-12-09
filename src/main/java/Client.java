import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {

    public static void main(String args[]) {

        Publisher publisher = new PublisherStub();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            String input = scanner.nextLine();
            if (input == null || input.equals("sair")) {
                System.out.println("Adeus");
                break;
            }

            String[] cmds = input.split(" ");

            switch (cmds[0].toLowerCase()) {
                case "help":
                    System.out.println("getSubs");
                case "getSubs":
                    publisher.getSubscriptions().thenAccept(subscriptions -> {
                        System.out.println("Subscrições:");
                        for (String s : subscriptions) {
                            System.out.println(s);
                        }
                    });
                case "get10":
                    publisher.getLast10().thenAccept(messages -> {
                        System.out.println("Subscrições:");
                        for (MessageReceive m : messages) {
                            System.out.println(m);
                        }
                    });
                case "addSub":
                    if(cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.addSubscription(cmds[1]);
                case "removeSub":
                    if(cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.removeSubscription(cmds[1]);
                case "publish":
                    if(cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    List<String> topics = new ArrayList<>();
                    for (int i = 2; i < cmds.length; i++) {
                        topics.add(cmds[i]);
                    }
                    publisher.publish(cmds[1],topics);
                default:
                    System.out.println("Comando não reconhecido");
            }
        }
        scanner.close();
    }

}
