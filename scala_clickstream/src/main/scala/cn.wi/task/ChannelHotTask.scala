package cn.wi.task

import cn.wi.`trait`.ProcessData
import cn.wi.pojo.{ChannelHot, Message}
import cn.wi.sink.ChannelHotSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelHotTask
 * @Author: xianlawei
 * @Description: 对传入的水位线数据进行数据转换，并数据落地到HBase中
 * @date: 2019/9/6 10:37
 */
object ChannelHotTask extends ProcessData {
  override def process(waterTimeDS: DataStream[Message]): Unit = {
    /**
     * 数据转换： 新建Bean：ChannelHot
     * 数据分组
     * 划分时间窗口
     * 数据聚合
     * 数据落地
     */

    //waterTimeDS是水位线拿到的数据内容，对数据进行转换成ChannelHot的Bean对象
    waterTimeDS.map((line: Message) =>
      ChannelHot(
        line.count,
        //channelId封装在UserBrowse中
        line.userBrowse.channelId,
        line.timeStamp))
      //数据分组
      .keyBy((line: ChannelHot) => line.channelId)
      //划分时间窗口
      .timeWindow(Time.seconds(3))
      //数据聚合  并将数据再次封装到ChannelHot中
      .reduce((x: ChannelHot, y: ChannelHot) => ChannelHot(x.count + y.count, x.channelId, x.timeStamp))
      //数据落地
      .addSink(new ChannelHotSink)
    //ChannelHotSink 继承了RichSinkFunction  才可以放入addSink
  }
}
