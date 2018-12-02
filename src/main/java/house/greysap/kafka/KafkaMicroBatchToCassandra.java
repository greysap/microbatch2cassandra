package house.greysap.kafka;

import house.greysap.model.CustomerTransaction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class KafkaMicroBatchToCassandra {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("microbatch2cassandra")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.connection.port", "9042")
                .master("local[2]")
                .getOrCreate();

        // Reduce number of tasks (the default is 200)
        spark.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

        // Schema of incoming JSON-files
        StructType schema = new StructType()
                .add("transactionId", DataTypes.LongType)
                .add("eventTime", DataTypes.TimestampType)
                .add("customerId", DataTypes.IntegerType)
                .add("transactionType", DataTypes.StringType) // Obviously, it's enough boolean for debit/credit.
                .add("amount", DataTypes.DoubleType);

        // Dataset with incoming records. Duplicated records will be ignored, if they arrive in the next 60 minutes.
        Dataset<CustomerTransaction> transactions = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("subscribe", "customer-transactions")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as message")
                .select(functions.from_json(functions.col("message"),schema).as("json"))
                .select("json.*")
                .filter("amount != 0")
                .withWatermark("eventTime", "60 minutes")
                .dropDuplicates("transactionId")
                .as(Encoders.bean(CustomerTransaction.class));

        // Dataframe with aggregates
        Dataset<Row> customerWindowedAggregates = transactions
                .withWatermark("eventTime", "24 hours")
                .groupBy(
                        transactions.col("customerId"),
                        transactions.col("transactionType"),
                        functions.window(transactions.col("eventTime"), "60 minutes")
                )
                .sum("amount")
                .toDF("customer_id", "transaction_type", "window", "total_amount");

        // Start running the query that writes windowed aggregates to Cassandra
        StreamingQuery query = customerWindowedAggregates
                .writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, batchId) -> rowDataset.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("cluster", "Test Cluster")
                        .option("keyspace", "sample")
                        .option("table", "transaction_aggregates")
                        .mode("append")
                        .save())
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("60 seconds"))
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}