package pl.put.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private final Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static final Scanner scanner = new Scanner(System.in);

	private static PreparedStatement SELECT_ALL_FROM_LIBRARY_DATA;
	private static PreparedStatement INSERT_BOOK;

	private static final String LIBRARY_DATA_FORMAT = "- %-10s %-10s %-10s %-10s\n";


	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_LIBRARY_DATA = session.prepare("SELECT * FROM library_data;");
			INSERT_BOOK = session
					.prepare("INSERT INTO library_data (library_name, library_location, book_name, author, book_count) VALUES (?, ?, ?, ?, ?);");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_LIBRARY_DATA);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String library_name = row.getString("library_name");
			String library_location = row.getString("library_location");
			String author = row.getString("author");
			String book_name = row.getString("book_name");

			builder.append(String.format(LIBRARY_DATA_FORMAT, library_name, library_location, author, book_name));
		}

		return builder.toString();
	}

	public void upsertBook() throws BackendException {
		System.out.println("Enter library: ");
		String libraryName = scanner.nextLine();

		System.out.println("Enter library location: ");
		String libraryLocation = scanner.nextLine();

		System.out.println("Enter book name: ");
		String bookName = scanner.nextLine();

		System.out.println("Enter author: ");
		String author = scanner.nextLine();

		System.out.println("Enter book count");
		int bookCount = scanner.nextInt();

		BoundStatement bs = new BoundStatement(INSERT_BOOK);
		bs.bind(libraryName, libraryLocation, bookName, author, bookCount);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

        logger.info("Book {} upserted", bookName);
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
