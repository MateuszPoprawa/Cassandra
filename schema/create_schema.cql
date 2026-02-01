CREATE KEYSPACE IF NOT EXISTS Library
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Library;

CREATE TABLE Library_Data (
library_id text,
book_id text,
book_count int,
queue map<text,timestamp>,
rented_date map<text,timestamp>,
due_date map<text,timestamp>,
PRIMARY KEY ((library_id), book_id)
); 
