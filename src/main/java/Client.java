import Middleware.MessageReceive;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Insira a porta do servidor:");
        int port = scanner.nextInt();

        Publisher publisher = new PublisherStub(Address.from(port));

        while (true) {
            String input = scanner.nextLine();
            if (input == null || input.equals("sair")) {
                System.out.println("Adeus");
                break;
            }
            String[] cmds = input.split(" ");

            switch (cmds[0].toLowerCase()) {
                case "help":
                    System.out.println("login <username> <password>");
                    System.out.println("register <username> <password>");
                case "login":
                    if(cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        continue;
                    }
                    publisher.login(cmds[1],cmds[2]).thenAccept(success ->  {
                        if(success) {
                            System.out.println("Login efetuado com sucesso");
                            session(scanner, publisher, cmds[1], cmds[2]);
                        }
                        else System.out.println("Login falhou");
                    });
                case "register":
                    if(cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        continue;
                    }
                    publisher.register(cmds[1],cmds[2]).thenAccept(success ->  {
                        if(success) System.out.println("Registado com sucesso");
                        else System.out.println("Não foi possível registar");
                    });
                default:
                    System.out.println("Comando não reconhecido");
            }
        }
        scanner.close();
    }

    private static void session(Scanner scanner, Publisher publisher, String username, String password) {

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
                    System.out.println("get10");
                    System.out.println("addSub <sub>");
                    System.out.println("removeSub <sub>");
                    System.out.println("publish <text> <tag1> <tag2>...");
                case "getSubs":
                    publisher.getSubscriptions(username, password).thenAccept(subscriptions -> {
                        System.out.println("Subscrições:");
                        for (String s : subscriptions) {
                            System.out.println(s);
                        }
                    });
                case "get10":
                    publisher.getLast10(username, password).thenAccept(messages -> {
                        System.out.println("Subscrições:");
                        for (MessageReceive m : messages) {
                            System.out.println(m);
                        }
                    });
                case "addSub":
                    if (cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.addSubscription(username, password, cmds[1]);
                case "removeSub":
                    if (cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.removeSubscription(username, password, cmds[1]);
                case "publish":
                    if (cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    List<String> topics = new ArrayList<>();
                    for (int i = 2; i < cmds.length; i++) {
                        topics.add(cmds[i]);
                    }
                    publisher.publish(username, password, cmds[1], topics);
                default:
                    System.out.println("Comando não reconhecido");
            }
        }
    }

}
