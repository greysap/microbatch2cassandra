package house.greysap;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ForeachBatchExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("microbatch2cassandra")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.connection.port", "9042")
                .master("local[2]")
                .getOrCreate();

        // Schema of incoming CSV-files
        StructType schema = new StructType()
                .add("transactionId", DataTypes.IntegerType)
                .add("eventTime", DataTypes.TimestampType)
                .add("customerId", DataTypes.IntegerType)
                .add("transactionType", DataTypes.StringType) // Obviously, it's enough boolean for debit/credit.
                .add("amount", DataTypes.DoubleType);

        // Dataset with incoming records. Duplicated records will be ignored, if they arrive in the next 10 minutes.
        Dataset<CustomerTransaction> transactions = spark
                .readStream()
                .option("sep", ",")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv("/tmp/incoming")
                .filter("amount != 0")
                .withWatermark("eventTime", "60 minutes")
                .dropDuplicates("transactionId", "eventTime")
                .as(ExpressionEncoder.javaBean(CustomerTransaction.class));

        // Table structure for results:
        // CREATE TABLE transaction_aggregates (customer_id int, total_amount double, transaction_type text, window text, PRIMARY KEY (customer_id, window, transaction_type));

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