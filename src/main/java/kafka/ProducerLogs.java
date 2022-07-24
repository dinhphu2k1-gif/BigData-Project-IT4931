package kafka;

import common.Common;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ProducerLogs implements Common {
    private final Producer<String, String> producer;

    private final String topic;

    private final Boolean isAsync;

    public static final String KAFKA_SERVER = "m1:9092,m2:9092";
    public static final String CLIENT_ID = "SampleProducer";

    public ProducerLogs(String topic, Boolean isAsync) {
        Properties props = new Properties();

        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("auto.create.topics.enable", false);
        props.put("client.id", CLIENT_ID);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    /**
     * Get all file in a directory
     *
     * @param directory
     * @return
     */
    public List<File> getListFiles(String directory) {
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
     * Send data to Kafka topic
     */
    public void sendData() {
        BufferedReader inputStream = null;
        JSONParser parser = new JSONParser();
        try {
            inputStream = new BufferedReader(
                    new FileReader("data.json"));
            String line;

            while ((line = inputStream.readLine()) != null) {
                JSONObject json = (JSONObject) parser.parse(line);
                String artist = (String) json.get("artist") != null ? (String) json.get("artist") : "";
                String auth = (String) json.get("auth") != null ? (String) json.get("auth") : "";
                String firstName = (String) json.get("firstName") != null ? (String) json.get("firstName") : "";
                String gender = (String) json.get("gender") != null ? (String) json.get("gender") : "";
                Long itemInSession = (Long) json.get("itemInSession") != null ? (Long) json.get("itemInSession") : -1;
                String lastName = (String) json.get("lastName") != null ? (String) json.get("lastName") : "";
                Double length = (Double) json.get("length") != null ? (Double) json.get("length") : -1;
                String level = (String) json.get("level") != null ? (String) json.get("level") : "";
                String location = (String) json.get("location") != null ? (String) json.get("location") : "";
                String method = (String) json.get("method") != null ? (String) json.get("method") : "";
                String page = (String) json.get("page") != null ? (String) json.get("page") : "";
                Long registration = (Long) json.get("registration") != null ? (Long) json.get("registration") : -1;
                Long sessionId = (Long) json.get("sessionId") != null ? (Long) json.get("sessionId") : -1;
                String song = (String) json.get("song") != null ? (String) json.get("song") : "";
                Long status = (Long) json.get("status") != null ? (Long) json.get("status") : -1;
                Long ts = (Long) json.get("ts") != null ? (Long) json.get("ts") : -1;
                String userAgent = (String) json.get("userAgent") != null ? (String) json.get("userAgent") : "";
                String userId = (String) json.get("userId") != null ? (String) json.get("userId") : "";

                String value = artist + "\t" + auth + "\t" + firstName + "\t" + gender + "\t" + itemInSession + "\t" + lastName + "\t"
                        + length + "\t" + level + "\t" + location + "\t" + method + "\t" + page + "\t" + registration + "\t" + sessionId + "\t"
                        + song + "\t" + status + "\t" + ts + "\t" + userAgent + "\t" + userId;
//                System.out.println(value);

                if (isAsync) {
                    producer.send(new ProducerRecord<>(topic, value), new ProdcucerCallback());
                }
                else {
                    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, line));

                    // We perform a get() on the future object, which turns the send call synchronous
                    RecordMetadata recordMetadata = future.get();

                    // The RecordMetadata object contains the offset and partition for the message.
                    System.out.println(String.format("Message written to partition %s with offset %s", recordMetadata.partition(),
                            recordMetadata.offset()));
                }

                Thread.sleep(250);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            producer.flush();
        }
    }

    public void run() {
        sendData();
    }

    public static void main(String[] args) {
        ProducerLogs producer = new ProducerLogs("music-logs", true);
        producer.run();
    }
}

