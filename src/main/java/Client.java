import Logic.Post;
import Logic.Publisher;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Insira a porta do cliente:");
        int port = scanner.nextInt();
        scanner.nextLine();
        Publisher publisher = new PublisherStub(port);

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
                    break;
                case "login":
                    if(cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        continue;
                    }
                    publisher.login(cmds[1],cmds[2]).thenAccept(success ->  {
                        if(success) {
                            System.out.println("Login efetuado com sucesso");
                            session(scanner, publisher, cmds[1]);
                        }
                        else System.out.println("Login falhou");
                    });
                    break;
                case "register":
                    if(cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        continue;
                    }
                    publisher.register(cmds[1],cmds[2]).thenAccept(success ->  {
                        if(success) System.out.println("Registado com sucesso");
                        else System.out.println("Não foi possível registar");
                    });
                    break;
                default:
                    System.out.println("Comando não reconhecido");
            }
        }
        scanner.close();
    }

    private static void session(Scanner scanner, Publisher publisher, String username) {

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
                    break;
                case "getsubs":
                    publisher.getSubscriptions(username).thenAccept(subscriptions -> {
                        System.out.println("Subscrições:");
                        if(subscriptions == null) {
                            System.out.println("erro");
                            return;
                        }
                        System.out.println(Arrays.toString(subscriptions.toArray()));
                    });
                    break;
                case "get10":
                    publisher.getLast10(username).thenAccept(messages -> {
                        System.out.println("Últimas 10 publicações:");
                        if(messages == null) {
                            System.out.println("erro");
                            return;
                        }
                        for (Post m : messages) {
                            System.out.println(m);
                        }
                    });
                    break;
                case "addsub":
                    if (cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.addSubscription(username, cmds[1]).thenRun(()->
                            System.out.println(cmds[1] + " subscrito")
                    );
                    break;
                case "removesub":
                    if (cmds.length < 2) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    publisher.removeSubscription(username, cmds[1]).thenRun(() ->
                            System.out.println(cmds[1] + " removido das subscrições")
                    );
                    break;
                case "publish":
                    if (cmds.length < 3) {
                        System.out.println("Argumentos insuficientes");
                        break;
                    }
                    List<String> topics = new ArrayList<>();
                    for (int i = 2; i < cmds.length; i++) {
                        topics.add(cmds[i]);
                    }
                    publisher.publish(username, cmds[1], topics).thenAccept(s -> System.out.println("mensagem publicada")).thenRun(() ->
                            System.out.println("mensagem publicada")
                    );
                    break;
                default:
                    System.out.println("Comando não reconhecido");
            }
        }
    }



}
