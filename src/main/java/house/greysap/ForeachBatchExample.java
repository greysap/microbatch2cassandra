package house.greysap;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ForeachBatchExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("microbatch2cassandra")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .config("spark.cassandra.connection.port", "9042")
                .master("local[1]")
                .getOrCreate();

        // Schema of incoming CSV-files
        StructType schema = new StructType()
                .add("transactionId", DataTypes.IntegerType)
                .add("date", DataTypes.DateType)
                .add("customerId", DataTypes.IntegerType)
                .add("amount", DataTypes.DoubleType);

        // Table structure for results: "CREATE TABLE transaction_aggregates (customer_id int PRIMARY KEY, total_amount double);"

        StreamingQuery query = spark.readStream()
                .option("sep", ",")
                .schema(schema)
                .csv("/tmp/incoming")
                .groupBy("customerId")
                .sum("amount")
                .toDF("customer_id", "total_amount")
                .writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, batchId) -> rowDataset.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("cluster", "Test Cluster")
                        .option("keyspace", "sample")
                        .option("table", "transaction_aggregates")
                        .mode("append")
                        .save())
                .outputMode("update")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}