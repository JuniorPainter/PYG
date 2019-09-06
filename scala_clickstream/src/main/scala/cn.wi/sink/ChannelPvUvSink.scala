package cn.wi.sink

import cn.wi.pojo.ChannelPvUv
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelPvUvSink
 * @Author: xianlawei
 * @Description: 实时的频道的PvUv的自定义数据落地
 * @date: 2019/9/6 14:09
 */
class ChannelPvUvSink extends RichSinkFunction[ChannelPvUv] {
  //执行HBase数据插入的操作
  override def invoke(value: ChannelPvUv): Unit = {
    /**
     * 落地数据： channelID ,time,pv,uv
     * 设计hbase表：
     * 表名：channel
     * 列族：info
     * 列名：channelId ,timeFormat,pv,uv
     * rowKey：channelId +time(格式化的日期)
     */

    val tableName: String = "channel"
    val family: String = "info"
    val pvColumnName: String = "pv"
    val uvColumnName: String = "uv"
    val rowKey: String = value.getChannelId + value.getTimeFormat

    //获取PV、Uv数据
    var pv: Long = value.getPv
    var uv: Long = value.getUv

    //插入之前先查询Hbase，如果有数据需要进行累加
    val pvString: String = HBaseUtil.queryByRowKey(tableName, family, pvColumnName, rowKey)
    val uvString: String = HBaseUtil.queryByRowKey(tableName, family, uvColumnName, rowKey)

    //非空判断
    if (StringUtils.isNotBlank(pvString)) {
      pv = pv + pvString.toLong
    }
    if (StringUtils.isNoneBlank(uvString)) {
      uv = uv + uvString.toLong
    }

    //封装多列数据，一次性插入
    var map: Map[String, Any] = Map[String, Any]()

    map += (pvColumnName -> pv)
    map += (uvColumnName -> uv)
    map += ("channelId" -> value.getChannelId)
    map += ("time" -> value.getTimeFormat)

    HBaseUtil.putMapDataByRowKey(tableName, family, map, rowKey)
  }
}
