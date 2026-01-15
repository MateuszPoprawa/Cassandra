CREATE KEYSPACE IF NOT EXISTS Library
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Library;

CREATE TABLE LibraryData (
libraryName text,
libraryLocation text,
bookName text,
bookCount int,
author text,
rentedDate map<text,timestamp>,
dueDate map<text,timestamp>,
PRIMARY KEY ((libraryName, libraryLocation), author, bookname)
); 
