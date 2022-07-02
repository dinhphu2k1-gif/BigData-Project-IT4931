package hust;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.swing.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ReadKafka {
    private final String kafkaServer = "172.17.0.2:9092,"
            + "172.17.0.3:9092,"
            + "172.17.0.4:9092";

    private final String topic = "music-logs";

//    public void run() {
//        SparkSession spark = SparkSession.builder().appName("test read").master("local").getOrCreate();
//
//        Dataset<Row> df = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", kafkaServer)
//                .option("subscribe", topic)
//                .option("failOnDataLoss", "false")
//                .load()
//                .selectExpr("CAST(value AS STRING) AS value");
//
//
//        df.show(false);
//    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.17.0.2:9092,"
                + "172.17.0.3:9092,"
                + "172.17.0.4:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // The extra properties we specify compared to when creating
        // a Kakfa producer
        props.put("auto.offset.reset", "latest");
        props.put("group.id", "DatajekConsumers");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe to all the topics matching the pattern
        consumer.subscribe(Arrays.asList("music-logs"));

        try {
            // We have shown an infinite loop for instructional purposes
            while (true) {
                Duration oneSecond = Duration.ofMillis(1000);

                // Poll the topic for new data and block for one second if new
                // data isn't available.
                ConsumerRecords<String, String> records = consumer.poll(oneSecond);

                // Loop through all the records received
                for (ConsumerRecord<String, String> record : records) {

                    String topic = record.topic();
                    int partition = record.partition();
                    long recordOffset = record.offset();
                    String key = record.key();
                    String value = record.value();

                    System.out.println("topic: %s" + topic + "\n" +
                            "partition: %s" + partition + "\n" +
                            "recordOffset: %s" + recordOffset + "\n" +
                            "key: %s" + key + "\n" +
                            "value: %s" + value);
                }
            }
        } finally {
            // Remember to close the consumer. If the consumer gracefully exits
            // the consumer group, the coordinator can trigger a rebalance immediately
            // and doesn't need to wait and detect that the consumer has left.
            consumer.close();
        }
    }
}
