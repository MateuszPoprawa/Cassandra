package pl.put.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.Date;

import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

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
	private static PreparedStatement RENT_BOOK;
	private static PreparedStatement RETURN_BOOK;
	private static PreparedStatement QUEUE_BOOK;
	private static PreparedStatement DEQUEUE_BOOK;
	private static PreparedStatement UNRENT_BOOK;

	private static final String LIBRARY_DATA_FORMAT = "%-15s %-15s %-15s\n";


	private void prepareStatements() throws BackendException {
		try {
			SELECT_BOOKS_FROM_LIBRARY = session.prepare("SELECT * FROM library_data WHERE library_id=?;");
			SELECT_BOOK = session.prepare("SELECT * FROM library_data " + "WHERE library_id=? AND book_id=?;");
			INSERT_BOOK = session.prepare("INSERT INTO library_data (library_id, book_id, book_count) VALUES (?, ?, ?);");
			RENT_BOOK = session.prepare("UPDATE library_data SET rented_date = rented_date + ?, due_date = due_date + ? WHERE library_id=? AND book_id=?;");
			RETURN_BOOK = session.prepare("UPDATE library_data SET queue = queue - ?, rented_date = rented_date - ?, due_date = due_date - ? WHERE library_id=? AND book_id=?;");
			QUEUE_BOOK = session.prepare("UPDATE library_data SET queue = queue + ? WHERE library_id=? AND book_id=?;");
			DEQUEUE_BOOK = session.prepare("UPDATE library_data SET queue = queue - ?,  rented_date = rented_date + ?, due_date = due_date + ? WHERE library_id=? AND book_id=? AND containsKey(queue, ?);");
			UNRENT_BOOK = session.prepare("UPDATE library_data SET queue = queue + ?,  rented_date = rented_date - ?, due_date = due_date - ? WHERE library_id=? AND book_id=? AND containsKey(rented_date, ?);");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	//---------------------------HANDLING INTERFACE------------------------------------------------------

	public void selectBooksFromLibrary() throws BackendException {

		String libraryId = getLibraryFromTerminal();

		ResultSet rs = selectBooksFromLibraryCassandra(libraryId);

		showResults(rs);
	}

	public void selectBook() throws BackendException {

		String libraryId = getLibraryFromTerminal();
		String bookId = getBookFromTerminal();

		ResultSet rs = selectBookCassandra(libraryId, bookId);

		showResults(rs);
	}

	public void upsertBook() throws BackendException {

		String libraryId = getLibraryFromTerminal();
		String bookId = getBookFromTerminal();
		int bookCount = getBookCountFromTerminal();

		upsertBookCassandra(libraryId, bookId, bookCount);

		scanner.nextLine();

        logger.info("Book {} upserted", bookId);
	}

	public int rentBook() throws BackendException{

		String userId = getUserFromTerminal();
		String libraryId = getLibraryFromTerminal();
		String bookId = getBookFromTerminal();

		ResultSet rs = selectBookCassandra(libraryId, bookId);

		int isRented = isBookRented(userId, rs);
		if(isRented > 0){
			System.out.println("This book has already been rented by this user.");
			return isRented;
		}

		rentBookCassandra(userId, libraryId, bookId);
		rs = validate(libraryId, bookId);
		int isRented = isBookRented(userId, rs);
		switch(isRented){
			case 0:
				System.out.println("Book is not rented, a return has been made during execution.");
				break;
			case 1:
				System.out.println("Book has been rented successfully.");
				break;
			case 2:
				System.out.println("You're in the queue to get the book. Check back later.");
				break;
		}
		return isRented;
	}

	public void returnBook() throws BackendException{

		String userId = getUserFromTerminal();
		String libraryId = getLibraryFromTerminal();
		String bookId = getBookFromTerminal();

		ResultSet rs = selectBookCassandra(libraryId, bookId);

		int isRented = isBookRented(userId, rs);
		switch(isRented){
			case 0:
				System.out.println("This book is not rented nor is this user in queue.");
				break;
			case 1: 
				returnBookCassandra(userId, libraryId, bookId);
				validate();
				System.out.println("Book returned.");
				break;
			case 2: 
				System.out.println("User removed from queue to get the book.");
				returnBookCassandra(userId, libraryId, bookId);
				validate();
				break;
		}
		return isRented;
	}

	//-----------------------------------PRIVATE STUFF--------------------------------------------------------------------------

	protected ResultSet validate(String libraryId, String bookId) throws BackendException{
		boolean isOk;
		ResultSet rs;
		do{
			isOk = true;
			rs = selectBookCassandra(libraryId, bookId);
			if(moveToQueue(rs)) isOk = false;
			if(moveFromQueue(rs)) isOk = false;
		}while(!isOk);
		return rs;
	}

	protected boolean moveToQueue(ResultSet rs) throws BackendException{
		boolean result = false;
		for (Row row : rs) {

			Map<String, Date> queue = row.getMap("queue", String.class, Date.class);
			Map<String, Date> rented = row.getMap("rented_date", String.class, Date.class);
			int bookCount = row.getInt("book_count");
			String libraryId = row.get("library_id");
			String bookId = row.get("book_id");

			if(rented.size() <= bookCount) continue;
			int diff = rented.size() - bookCount;

			for(int i = 0; i < diff; i++){
				String nextUser = null;
				Date nextDate = null;
				for(String userId : rented.keySet()){
					Date date = rented.get(userId);
					if(nextUser == null){
						nextUser = userId;
						nextDate = date;
						continue;
					}
					if(date > nextDate){
						nextUser = userId;
						nextDate = date;
					}
				}
				unrentBookCassandra(nextUser, libraryId, bookId);
				result = true;
			}
		}
		return result;
	}

	protected boolean moveFromQueue(ResultSet rs) throws BackendException{
		boolean result = false;
		for (Row row : rs) {

			Map<String, Date> queue = row.getMap("queue", String.class, Date.class);
			Map<String, Date> rented = row.getMap("rented_date", String.class, Date.class);
			int bookCount = row.getInt("book_count");
			String libraryId = row.get("library_id");
			String bookId = row.get("book_id");

			if(rented.size() >= bookCount) continue;

			int diff = bookCount - rented.size();
			diff = Math.min(diff, queue.size());

			for(int i = 0; i < diff; i++){
				String nextUser = null;
				Date nextDate = null;
				for(String userId : queue.keySet()){
					Date date = queue.get(userId);
					if(nextUser == null){
						nextUser = userId;
						nextDate = date;
						continue;
					}
					if(date < nextDate){
						nextUser = userId;
						nextDate = date;
					}
				}
				dequeueBookCassandra(nextUser, libraryId, bookId);
				result = true;
			}
		}
		return result;
	}

	protected int isBookRented(String userId, ResultSet rs) throws BackendException{
		for (Row row : rs) {
			Map<String, Date> queue = row.getMap("queue", String.class, Date.class);
			Map<String, Date> rented = row.getMap("rented_date", String.class, Date.class);
			if(rented.containsKey(userId)) return 1;
			if(queue.containsKey(userId) )return 2;
		}
		return 0;
	}

	protected void finalize() throws BackendException{
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

	protected void showResults(ResultSet rs) throws BackendException {
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(LIBRARY_DATA_FORMAT, "LIBRARY_ID", "BOOK_ID", "BOOK_COUNT"));

		for (Row row : rs) {
			String libraryId = row.getString("library_id");
			String bookId = row.getString("book_id");
			int bookCount = row.getInt("book_count");

			builder.append(String.format(LIBRARY_DATA_FORMAT, libraryId, bookId, bookCount));
		}

		System.out.println(builder);
	}

	//----------------------------CASSANDRA QUERY EXECUTION--------------------------------------------------------------------------

	protected ResultSet selectBookCassandra(String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(SELECT_BOOK);
		bs.bind(libraryId, bookId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet upsertBookCassandra(String libraryId, String bookId, int bookCount) throws BackendException{
		BoundStatement bs = new BoundStatement(INSERT_BOOK);
		bs.bind(libraryId, bookId, bookCount);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet selectBooksFromLibraryCassandra(String libraryId) throws BackendException{
		BoundStatement bs = new BoundStatement(SELECT_BOOKS_FROM_LIBRARY);
		bs.bind(libraryId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet rentBookCassandra(String userId, String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(RENT_BOOK);
		Map<String, Date> myMap = new HashMap<>();
		myMap.put(userId, new Date());
		bs.bind(myMap, myMap, libraryId, bookId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet returnBookCassandra(String userId, String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(RETURN_BOOK);
		Set<String> mySet = new HashSet<>();
		mySet.add(userId);
		bs.bind(mySet, mySet, mySet, libraryId, bookId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet queueBookCassandra(String userId, String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(QUEUE_BOOK);
		Map<String, Date> myMap = new HashMap<>();
		myMap.put(userId, new Date());
		bs.bind(myMap, libraryId, bookId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet dequeueBookCassandra(String userId, String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(DEQUEUE_BOOK);
		Set<String> mySet = new HashSet<>();
		mySet.add(userId);
		Map<String, Date> myMap = new HashMap<>();
		myMap.put(userId, new Date());
		bs.bind(mySet, myMap, myMap, libraryId, bookId, userId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet unrentBookCassandra(String userId, String libraryId, String bookId) throws BackendException{
		BoundStatement bs = new BoundStatement(DEQUEUE_BOOK);
		Set<String> mySet = new HashSet<>();
		mySet.add(userId);
		Map<String, Date> myMap = new HashMap<>();
		myMap.put(userId, new Date());
		bs.bind(myMap, mySet, mySet, libraryId, bookId, userId);
		ResultSet rs;
		rs = executeQuery(bs);
		return rs;
	}

	protected ResultSet executeQuery(BoundStatement bs) throws BackendException{
		ResultSet rs;
		try {
			rs = session.execute(bs);
			return rs;
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

	//------------------TERMINAL INPUTS-------------------------------------------------------------------------------------------------

	protected String getUserFromTerminal() throws BackendException{
		System.out.println("Enter user id: ");
		String userId = scanner.nextLine();
		return userId;
	}

	protected String getLibraryFromTerminal() throws BackendException{
		System.out.println("Enter library id: ");
		String LibraryId = scanner.nextLine();
		return LibraryId;
	}

	protected String getBookFromTerminal() throws BackendException{
		System.out.println("Enter book id: ");
		String bookId = scanner.nextLine();
		return bookId;
	}

	protected int getBookCountFromTerminal() throws BackendException{
		System.out.println("Enter book count: ");
		int bookCount = scanner.nextInt();
		return bookCount;
	}

}
