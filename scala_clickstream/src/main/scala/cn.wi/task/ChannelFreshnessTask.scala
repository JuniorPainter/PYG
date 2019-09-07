package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.map.ChannelFreshnessFlatMap
import cn.wi.pojo.{ChannelFreshness, Message}
import cn.wi.reduce.ChannelFreshnessReduce
import cn.wi.sink.ChannelFreshnessSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelFreshnessTask
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 15:26
 */
object ChannelFreshnessTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    /**
     * 1. 数据转换-->ChannelFreshnessFlatMap
     * 2. 数据分组
     * 3. 划分时间窗口
     * 4. 数据聚合
     * 5. 数据落地
     */


    waterTimeDS
      //自定义数据转换
      .flatMap(new ChannelFreshnessFlatMap)
      //自定义数据分组
      .keyBy((line: ChannelFreshness) => line.channelId + line.timeFormat)
      //划分时间窗口
      .timeWindow(Time.seconds(3))
      //自定义数据聚合
      .reduce(new ChannelFreshnessReduce)
    //自定义数据落地
      .addSink(new ChannelFreshnessSink)
  }
}
