package Week5;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> record = null;
        for(int a = 0 ; a <= 12 ; a++){
            String data = "This is a record" + a ;
            record = new ProducerRecord<String,String>("Test-Producer",data);
            producer.send(record);
        }
        producer.flush();
        producer.close();



    }
}
