Private function to build DDL for table recreation with metadata.

`CREATE TABLE LIKE` is not suitable when the source table query and target table schema is different.
This SQL generator's goal is to generate DDL for table recreation with metadata like `CREATE TABLE LIKE` operator.
