package cn.wi.sink

import cn.wi.pojo.ChannelNetwork
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelNetworkSink
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:29
 */
class ChannelNetworkSink extends RichSinkFunction[ChannelNetwork] {
  /**
   * 数据落地：channelId, network,time,newCount,oldCount
   * 设计hbase表
   * 表名：network
   * 列族：info
   * 列名：channelId, network,time,newCount,oldCount
   * rowKey：channelId +time(格式化的日期)
   */
  override def invoke(value: ChannelNetwork): Unit = {
    val tableName: String = "network"
    val family: String = "info"
    val newCountCol: String = "newCount"
    val oldCountCol: String = "oldCount"
    val rowKey: String = value.getChannelId + value.getTimeFormat

    var newCount: Long = value.getNewCount
    var oldCount: Long = value.getOldCount

    //通过hbase查询新老用户的数据，如果有数据，需要进行累加操作
    val newCountData: String = HBaseUtil.queryByRowKey(tableName, family, newCountCol, rowKey)
    val oldCountData: String = HBaseUtil.queryByRowKey(tableName, family, oldCountCol, rowKey)
    //非空判断，及累加操作
    if (StringUtils.isNotBlank(newCountData)) {
      newCount = newCount + newCountData.toLong
    }
    if (StringUtils.isNotBlank(oldCountData)) {
      oldCount = oldCount + oldCountData.toLong
    }

    //将插入的多列数据，封装进map
    //列名：channelId, network,time,newCount,oldCount
    var map = Map[String, Any]()
    map += ("channelId" -> value.getChannelId)
    map += ("network" -> value.getNetWork)
    map += ("time" -> value.getTimeFormat)
    map += (newCountCol -> newCount)
    map += (oldCountCol -> oldCount)

    //执行插入操作
    HBaseUtil.putMapDataByRowKey(tableName, family, map, rowKey)


  }
}
