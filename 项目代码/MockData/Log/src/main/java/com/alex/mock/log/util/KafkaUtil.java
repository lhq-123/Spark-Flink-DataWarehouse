
package com.alex.mock.log.util;

import java.util.Properties;

import com.alex.mock.log.config.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaUtil
{

    public KafkaUtil()
    {
    }

    public static KafkaProducer createKafkaProducer()
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", AppConfig.kafka_server);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = null;
        try
        {
            producer = new KafkaProducer(properties);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg)
    {
        if(kafkaProducer == null)
            kafkaProducer = createKafkaProducer();
        kafkaProducer.send(new ProducerRecord(topic, msg));
        System.out.println(msg);
    }

    public static KafkaProducer kafkaProducer = null;

}
