package kafka;

import common.Common;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class ProducerLogs implements Common {
    private final String directory = "file://" + System.getProperty("user.dir") + "/data/log_data";

    private SparkSession spark;

    /**
     * Get all file in a directory
     *
     * @param directory
     * @return
     */
    public List<File> getListFiles(String directory) {
        System.out.println(Paths.get(directory));
        List<File> filesInFolder;
        try {
            filesInFolder = Files.walk(Paths.get(directory))
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return filesInFolder;
    }

    /**
     * Read data
     *
     * @param path
     * @return
     */
    public Dataset<String> readData(String path) {
        Dataset<Row> df = spark.read()
                .json(path);

//        df.printSchema();

        df = df.na().fill("");

        df = df.select(concat_ws("\t"
                , col("artist")
                , col("auth")
                , col("firstName")
                , col("gender")
                , col("itemInSession")
                , col("lastName")
                , col("length")
                , col("level")
                , col("level")
                , col("location")
                , col("method")
                , col("page")
                , col("registration")
                , col("sessionId")
                , col("song")
                , col("status")
                , col("ts") //timestamp
                , col("userAgent")
                , col("userId")));

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> res = df.map((MapFunction<Row, String>) row -> row.getString(0), stringEncoder);

        return res;
    }

    /**
     * Send data to Kafka topic
     */
    public void sendData() {
        List<File> files = this.getListFiles(directory);

        for (File file : files) {
            Dataset<String> df = this.readData(file.getAbsolutePath());

            df.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("topic", topic)
                    .save();

            System.out.println("Finish processing file :" + file.getName());
        }
    }

    public void run() {
        this.spark = SparkSession.builder()
                .appName("Send data to Kafka")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        sendData();
    }

    public static void main(String[] args) {
        ProducerLogs producer = new ProducerLogs();
        producer.run();
    }
}
