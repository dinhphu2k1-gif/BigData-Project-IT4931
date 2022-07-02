package hust;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Test")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("file:///home/dinhphu/bigdata-project/data/log_data/2018/11/2018-11-01-events.json");

        df.printSchema();
        df = df.withColumn("time", col("ts").cast(TimestampType))
                .drop("ts");

        df.show();
    }
}
