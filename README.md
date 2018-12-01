# microbatch2cassandra
## Simple example of using Spark Structured Streaming for imaginary case of aggregating customers bank transactions

### Case description
Our customer can make debit and credit transactions. Our task is to aggregate all of them in one-hour windows and save aggregates to a database. "To aggregate" means to group transactions by customer, time window, type... and sum up.
Transactions arrive as CSV-files. Apache Cassandra is used as aggregates storage.

### Few complications:
1. Transactions may be incorrect (malformed CSV, zero amount).
2. Transaction may be duplicated in one hour. And we mustn't sum up one transaction twice.
3. Transactions may be late for few hours, but no more than 24 hours.

### Solution:
1. Read stream of CSV-files and filter off incorrect records.
2. Use time-window aggregation.
3. Write batches of aggregates to Cassandra via spark-cassandra-connector.