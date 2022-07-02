package kafka;

import common.Common;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.col;

public class ConsumerLogs implements Common {
    private SparkSession spark;

    private final String destination = "hdfs://m1:8020/data";

    public void readData() {
        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");

//        df.write().format("parquet").save(System.getProperty("user.dir") + "/dataKafka");
        df = df.select(split(col("value"), "\t").as("split(value)"));

//        df.show(false);

        Dataset<Row> midDF = df.select(
                        col("split(value)")
                                .getItem(0)
                                .cast("string")
                                .as("artist"),
                        col("split(value)")
                                .getItem(1)
                                .cast("string")
                                .as("auth"),
                        col("split(value)")
                                .getItem(2)
                                .cast("string")
                                .as("firstName"),
                        col("split(value)")
                                .getItem(3)
                                .cast("string")
                                .as("gender"),
                        col("split(value)")
                                .getItem(4)
                                .cast("long")
                                .as("itemInSession"),
                        col("split(value)")
                                .getItem(5)
                                .cast("string")
                                .as("lastName"),
                        col("split(value)")
                                .getItem(6)
                                .cast("double")
                                .as("length"),
                        col("split(value)")
                                .getItem(7)
                                .cast("string")
                                .as("level"),
                        col("split(value)")
                                .getItem(8)
                                .cast("string")
                                .as("location"),
                        col("split(value)")
                                .getItem(9)
                                .cast("string")
                                .as("method"),
                        col("split(value)")
                                .getItem(10)
                                .cast("string")
                                .as("page"),
                        col("split(value)")
                                .getItem(11)
                                .cast("double")
                                .as("registration"),
                        col("split(value)")
                                .getItem(12)
                                .cast("long")
                                .as("sessionId"),
                        col("split(value)")
                                .getItem(13)
                                .cast("string")
                                .as("song"),
                        col("split(value)")
                                .getItem(14)
                                .cast("long")
                                .as("status"),
                        col("split(value)")
                                .getItem(15)
                                .cast("long")
                                .divide(1000)
                                .cast("timestamp")
                                .as("ts"),
                        col("split(value)")
                                .getItem(16)
                                .cast("string")
                                .as("userAgent"),
                        col("split(value)")
                                .getItem(17)
                                .as("string")
                                .as("userId")
                )
                .withColumn("year", col("ts").substr(0, 4))
                .withColumn("month", col("ts").substr(6, 2))
                .withColumn("day", col("ts").substr(9, 2));

        midDF.show();

        midDF.write().format("parquet").partitionBy("year", "month", "day").save( "hdfs:/dataKafka");
    }

    public void run() {
        this.spark = SparkSession.builder()
                .appName("Upload data to HDFS")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        readData();
    }

    public static void main(String[] args) {
        ConsumerLogs comsumerLogs = new ConsumerLogs();
        comsumerLogs.run();
    }
}
