package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.map.ChannelBrowseFlatMap
import cn.wi.pojo.{ChannelBrowse, Message}
import cn.wi.reduce.ChannelBrowseReduce
import cn.wi.sink.ChannelBrowseSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelBrowseTask
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 19:20
 */
object ChannelBrowseTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    waterTimeDS
      .flatMap(new ChannelBrowseFlatMap)
      .keyBy((line: ChannelBrowse) => line.getChannelId + line.getTimeFormat)
      .timeWindow(Time.seconds(3))
      .reduce(new ChannelBrowseReduce)
      .addSink(new ChannelBrowseSink)
  }
}
