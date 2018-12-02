package house.greysap.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class CustomerTransactionsProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "3");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newTransaction(123, "DEBIT", 100.0));
                Thread.sleep(1000);
                producer.send(newTransaction(321, "CREDIT", -10.0));
                Thread.sleep(1000);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    private static ProducerRecord<String, String> newTransaction(int customerId, String transactionType, double amount) {
        long transactionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        String eventTime = new Timestamp(System.currentTimeMillis()).toString();

        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        transaction.put("transactionId", transactionId);
        transaction.put("eventTime", eventTime);
        transaction.put("customerId", customerId);
        transaction.put("transactionType", transactionType);
        transaction.put("amount", amount);
        return new ProducerRecord<>("customer-transactions", String.valueOf(transactionId), transaction.toString());
    }
}