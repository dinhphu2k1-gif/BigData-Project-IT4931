package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProdcucerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        } else {
            // The RecordMetadata object contains the offset and partition for the message.
            System.out.println(String.format("Message written to partition %s with offset %s", recordMetadata.partition(),
                    recordMetadata.offset()));
        }
    }
}
