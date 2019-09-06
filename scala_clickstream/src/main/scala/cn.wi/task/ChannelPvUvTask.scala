package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.map.ChannelPvUvFlatMap
import cn.wi.pojo.{ChannelPvUv, Message}
import cn.wi.reduce.ChannelPvUvReduce
import cn.wi.sink.ChannelPvUvSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelPvUvTask
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 13:34
 */
object ChannelPvUvTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    /**
     * 1. 数据转换
     * 2. 分组
     * 3。 划分时间窗口
     * 4. 数据聚合
     * 5. 数据落地
     */

    //数据转换 ChannelPvUvFlatMap自定义数据转换
    waterTimeDS.flatMap(new ChannelPvUvFlatMap)
      //分组
      .keyBy((line: ChannelPvUv) => line.channelId + line.getTimeFormat)
      //划分时间窗口
      .timeWindow(Time.seconds(3))
    //数据聚合
      .reduce(new ChannelPvUvReduce)
    //数据落地
      .addSink(new ChannelPvUvSink)
  }
}
