package pl.put.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.time.LocalDateTime;

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

	private static PreparedStatement SELECT_BOOKS_FROM_LIBRARY;
	private static PreparedStatement SELECT_BOOK;
	private static PreparedStatement INSERT_BOOK;

	private static final String LIBRARY_DATA_FORMAT = "%-15s %-15s %-15s\n";


	private void prepareStatements() throws BackendException {
		try {
			SELECT_BOOKS_FROM_LIBRARY = session.prepare("SELECT * FROM library_data WHERE library_id=?;");
			SELECT_BOOK = session.prepare("SELECT * FROM library_data " + "WHERE library_id=? AND book_id=?;");
			INSERT_BOOK = session.prepare("INSERT INTO library_data (library_id, book_id, book_count) VALUES (?, ?, ?);");
			RENT_BOOK = session.prepare("UPDATE library_data SET rented_date = rented_date + {'?':'?'}, due_date = due_date + {'?':'?'} WHERE library_id=? AND book_id=?;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public void selectBooksFromLibrary() throws BackendException {
		StringBuilder builder = new StringBuilder();

		System.out.println("Enter library id: ");
		String libraryId = scanner.nextLine();

		BoundStatement bs = new BoundStatement(SELECT_BOOKS_FROM_LIBRARY);
		bs.bind(libraryId);

		showResults(builder, bs);
	}

	public void selectBook() throws BackendException {
		StringBuilder builder = new StringBuilder();

		System.out.println("Enter library id: ");
		String libraryId = scanner.nextLine();

		System.out.println("Enter book id: ");
		String bookId = scanner.nextLine();

		BoundStatement bs = new BoundStatement(SELECT_BOOK);
		bs.bind(libraryId, bookId);

		showResults(builder, bs);
	}

	public void upsertBook() throws BackendException {

		System.out.println("Enter library id: ");
		String libraryId = scanner.nextLine();

		System.out.println("Enter book id: ");
		String bookId = scanner.nextLine();

		System.out.println("Enter book count");
		int bookCount = scanner.nextInt();

		BoundStatement bs = new BoundStatement(INSERT_BOOK);
		bs.bind(libraryId, bookId, bookCount);

		executeQuery(bs);

		scanner.nextLine();

        logger.info("Book {} upserted", bookId);
	}

	public void rentBook(){
		System.out.println("Enter user id: ");
		String userId = scanner.nextLine();

		System.out.println("Enter library id: ");
		String libraryId = scanner.nextLine();

		System.out.println("Enter book id: ");
		String bookId = scanner.nextLine();

		BoundStatement bs = new BoundStatement(SELECT_BOOK);
		bs.bind(libraryId, bookId);
		ResultSet rs;
		rs.executeQuery();

		bs = new BoundStatement(userId, LocalDateTime.now());
		bs.bind(userId, LocalDateTime.now().toString(), userId, LocalDateTime.now().toString(), library_id, book_id);
		executeQuery(bs);
		verify(libraryId, bookId);
	}

	protected void verify(String libraryId, String bookId){
		boolean isOk = true;
		do{
			BoundStatement bs = new BoundStatement(SELECT_BOOK);
			bs.bind(libraryId, bookId);
			ResultSet rs;
			rs = executeQuery(bs);
			for (Row row : rs){
				break;
			}
		}while(!isOk);
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

	private void showResults(StringBuilder builder, BoundStatement bs) throws BackendException {
		ResultSet rs;

		rs = executeQuery(bs);

		builder.append(String.format(LIBRARY_DATA_FORMAT, "LIBRARY_ID", "BOOK_ID", "BOOK_COUNT"));

		for (Row row : rs) {
			String library_name = row.getString("library_id");
			String book_name = row.getString("book_id");
			int book_count = row.getInt("book_count");

			builder.append(String.format(LIBRARY_DATA_FORMAT, library_id, book_id, book_count));
		}

		System.out.println(builder);
	}

	private ResultSet executeQuery(BoundStatement bs){
		ResultSet rs;
		try {
			rs = session.execute(bs);
			Return rs;
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

}
