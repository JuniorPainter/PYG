package cn.wi.sink

import cn.wi.pojo.ChannelFreshness
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelFreshnessSink
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 15:49
 */
class ChannelFreshnessSink extends RichSinkFunction[ChannelFreshness] {

  //插入HBase
  override def invoke(value: ChannelFreshness): Unit = {
    /**
     * 数据落地
     * 设计hbase表：
     * 表名：channel
     * 列族：info
     * 列名：channelId ,time,newCount,oldCount
     * rowKey：channelId +time(格式化的日期)
     */

    //设置HBase表的参数
    val tableName: String = "channel"
    val family: String = "info"
    val newCountColumn: String = "newCount"
    val oldCountColumn: String = "oldCount"
    val rowKey: String = value.getChannelId + value.timeFormat

    //取newCount和oldCount
    var newCount: Long = value.getNewCount
    var oldCount: Long = value.getOldCount

    //查询Hbase的新老用户数据，如果有数据，需要进行累加操作
    val newCountString: String = HBaseUtil.queryByRowKey(tableName, family, newCountColumn, rowKey)
    val oldCountString: String = HBaseUtil.queryByRowKey(tableName, family, oldCountColumn, rowKey)

    //非空判断
    if (StringUtils.isNotBlank(newCountString)) {
      newCount = newCount + newCountString.toLong
    }
    if (StringUtils.isNotBlank(oldCountString)) {
      oldCount = oldCount + oldCountString.toLong
    }

    //落地数据封装到map
    var map: Map[String, Any] = Map[String, Any]()
    map += ("channelId" -> value.getChannelId)
    map += ("time" -> value.getTimeFormat)
    map += (newCountColumn -> newCount)
    map += (oldCountColumn -> oldCount)

    HBaseUtil.putMapDataByRowKey(tableName, family, map, rowKey)
  }
}
