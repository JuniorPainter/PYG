package cn.wi.util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * @author xianlawei
 * @Date 2019/9/5
 */
public class KafkaUtil {

    /**
     * 获取kafka生产者对象
     *
     */
    private static Producer<String, String> getProducer() {
        Properties prop = new Properties();
        prop.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        //broker的地址
        prop.put("metadata.broker.list", "node01:9092,node02:9092,node03:9092");
        prop.put("serializer.class", StringEncoder.class.getName());
        ProducerConfig producerConfig = new ProducerConfig(prop);
        return new Producer<String, String>(producerConfig);
    }

    /**
     * 发送数据到kafka
     */
    public static void sendData(String topic, String key, String value) {

        Producer<String, String> producer = getProducer();
        producer.send(new KeyedMessage<>(topic, key, value));
    }


}
