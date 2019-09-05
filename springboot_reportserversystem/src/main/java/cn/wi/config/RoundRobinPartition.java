package cn.wi.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ProjectName: Flink_PYG
 * @ClassName: RoundRobinPartition
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/5 15:45
 */
public class RoundRobinPartition implements Partitioner {
    /**
     * 使用给定的初始值创建一个新的AtomicInteger   自增长int类型
     */
    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //分区数量
        Integer partitionCount = cluster.partitionCountForTopic(topic);
        //该方法每次调用都会自增1  取模长  就会实现
        //比如此处分区数为3  则取的模 为 0  1  2
        int i = atomicInteger.incrementAndGet() % partitionCount;

        //当自增长达到一千的时候，重新赋予初始值
        int number = 1000;
        if (atomicInteger.get() > number) {
            atomicInteger.set(0);
        }
        return i;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
