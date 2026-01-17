package pl.put;

import java.io.IOException;
import java.util.Properties;

import pl.put.backend.BackendException;
import pl.put.backend.BackendSession;

public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws IOException, BackendException {
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

        session.upsertBook("library1", "Poznań", "J.R.R. Tolkien", "The Lord of the Rings");
        session.upsertBook("library1", "Poznań", "Frank Herbert", "Dune" );

        String output = session.selectAll();
        System.out.println("Books: \n" + output);

        session.deleteAll();

        System.exit(0);
    }
}
