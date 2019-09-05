package cn.wi.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;

/**
 * @ProjectName: Flink_PYG
 * @ClassName: KafkaConfiguration
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/4 22:45
 * 配置文件：SpringBoot中是没有XML配置文件
 */
@Configuration
@EnableKafka
public class KafkaConfiguration {

    /**
     * 注入属性
     */
    @Value("${kafka.producer.servers}")
    private String severs;

    @Value("${kafka.producer.retries}")
    private int retries;

    @Value("${kafka.producer.batch.size}")
    private int batchSize;

    @Value("${kafka.producer.linger}")
    private int linger;

    @Value("${kafka.producer.buffer.memory}")
    private int memory;

    @Bean
    public KafkaTemplate kafkaTemplate() {
        //添加kafka配置
        HashMap<String, Object> map = new HashMap<String, Object>(16);
        //broker地址
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, severs);
        map.put(ProducerConfig.RETRIES_CONFIG, retries);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        map.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, memory);

        //需要配置KV的序列化
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //避免数据倾斜
        //算法RoundRobin 轮询的算法    RoundRobinPartition 实现轮询算法的类
        map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartition.class);

        //kafka核心配置对象  DefaultKafkaConsumerFactory 需要一个Map的参数
        // DefaultKafkaProducerFactory实现了ProducerFactory接口
        ProducerFactory kafkaProducerFactory = new DefaultKafkaProducerFactory(map);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<String, String>(kafkaProducerFactory);
        return kafkaTemplate;
    }

}
