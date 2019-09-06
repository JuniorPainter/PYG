package cn.wi.sink

import cn.wi.pojo.ChannelHot
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelHotSink
 * @Author: xianlawei
 * @Description: 频道热点的自定义数据落地
 *
 *               落地数据： channelID ,count
 *               设计hbase表：
 *               表名：channel
 *               列族：info
 *               列名：channelId ,count
 *               rowKey：channelId
 * @date: 2019/9/6 10:49
 */
// 输入时ChannelHot
class ChannelHotSink extends RichSinkFunction[ChannelHot] {
  /**
   * 执行数据插入操作
   */
  override def invoke(value: ChannelHot): Unit = {
    val tableName: String = "channel"
    val family: String = "info"
    val columnName: String = "count"
    //channelId存在value里面可以直接获取
    val rowKey: Long = value.channelId
    var count: Int = value.count

    //需要先查询HBase，如果Hbase里有count数据，需要累加，再插入数据
    val resultString: String = HBaseUtil.queryByRowKey(tableName, family, columnName, rowKey.toString)
    //非空判断 如果HBase中有数据就累加  没有就将count直接插入
    if (StringUtils.isNoneBlank(resultString)) {
      count = count + resultString.toInt
    }
    //执行插入操作
    val map: Map[String, Long] = Map(columnName -> count, "channelId" -> value.channelId)
    HBaseUtil.putMapDataByRowKey(tableName, family, map, rowKey.toString)
  }
}
