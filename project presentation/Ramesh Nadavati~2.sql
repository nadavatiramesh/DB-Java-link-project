CREATE TYPE Book_t AS OBJECT
( name VARCHAR2(100),
  author VARCHAR2(30),
  abstract VARCHAR2(1000));

CREATE TYPE BookSet_t AS TABLE OF Book_t;

CREATE TABLE Catalogs
( name VARCHAR2(30), 
  cat CLOB);
----Function GetBooks is defined as follows:

CREATE FUNCTION GetBooks(a CLOB) RETURN BookSet_t;
---The following query returns all the catalogs and their corresponding book listings.

SELECT c.name, Book.name, Book.author, Book.abstract
  FROM Catalogs c, TABLE(GetBooks(c.cat)) Book;