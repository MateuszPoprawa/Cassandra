package pl.put;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import pl.put.backend.BackendException;
import pl.put.backend.BackendSession;

public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws BackendException {
        String contactPoint = null;
        String keyspace = null;

        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

            contactPoint = properties.getProperty("contact_point");
            keyspace = properties.getProperty("keyspace");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        BackendSession session = new BackendSession(contactPoint, keyspace);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            showInfo();
            int option = scanner.nextInt();
            switch (option) {
                case 0: System.exit(0);
                case 1: session.upsertBook(); break;
                case 2: session.selectBook(); break;
                case 3: session.selectBooksFromLibrary(); break;
                case 4: session.selectBook(); break;
                case 5: session.selectBook(); break;
                case 6: session.rentBook(); break;
                case 7: session.returnBook(); break;
                default: break;
            }
        }

    }

    private static void showInfo() {
        System.out.println("Choose option: ");
        System.out.println("0 - exit");
        System.out.println("1 - Insert book");
        System.out.println("2 - Select book");
        System.out.println("3 - Select books from library");
        System.out.println("4 - Delete book");
        System.out.println("5 - Update book");
        System.out.println("6 - Rent book");
        System.out.println("7 - Return book");
    }
}
