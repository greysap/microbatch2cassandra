# microbatch2cassandra
Simple example of using Spark Structured Streaming for 
1. Reading CSV-files with customers' transactions.
2. Aggregating them in memory.
3. Batch writing aggregates to Cassandra via spark-cassandra-connector.
