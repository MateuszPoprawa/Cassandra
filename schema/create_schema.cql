CREATE KEYSPACE IF NOT EXISTS Library
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Library;

CREATE TABLE Library_Data (
library_name text,
library_location text,
book_name text,
book_count int,
author text,
rented_date map<text,timestamp>,
due_date map<text,timestamp>,
PRIMARY KEY ((library_name, library_location), book_name, author)
); 
