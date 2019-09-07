package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.map.ChannelRegionFlatMap
import cn.wi.pojo.{ChannelRegion, Message}
import cn.wi.reduce.ChannelRegionReduce
import cn.wi.sink.ChannelRegionSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelRegionTask
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 16:13
 */
object ChannelRegionTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    /**
     * 数据转换：channelRegion
     * 数据分组
     * 划分时间窗口
     * 数据聚合
     * 数据落地
     */

    waterTimeDS
      .flatMap(new ChannelRegionFlatMap)
      .keyBy((line: ChannelRegion) => line.channelId + line.getTimeFormat)
      .timeWindow(Time.seconds(3))
      .reduce(new ChannelRegionReduce)
      .addSink(new ChannelRegionSink)
  }
}
