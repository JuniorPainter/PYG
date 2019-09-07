package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.map.ChannelNetworkFlatMap
import cn.wi.pojo.{ChannelNetwork, Message}
import cn.wi.reduce.ChannelNetworkReduce
import cn.wi.sink.ChannelNetworkSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelNetworkTask
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:03
 */
object ChannelNetworkTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    /**
     * 1. 数据转换：Bean ChannelNetwork
     * 2. 分组 ChannelId + timeFormat
     * 3. 划分时间窗口
     * 4. 数据聚合
     * 5. 数据落地
     */

    waterTimeDS
      .flatMap(new ChannelNetworkFlatMap)
      .keyBy((line: ChannelNetwork) => line.getChannelId + line.getTimeFormat)
      .timeWindow(Time.seconds(3))
      .reduce(new ChannelNetworkReduce)
      .addSink(new ChannelNetworkSink)
  }
}
