package cn.wi.sink

import cn.wi.pojo.ChannelBrowse
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelBrowseSink
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:48
 */
class ChannelBrowseSink extends RichSinkFunction[ChannelBrowse]{

  override def invoke(value: ChannelBrowse): Unit = {

    /**
     * 数据落地：channelId, browser,time,pv,uv,newCount,oldCount
     * 设计hbase表：
     * 表名：browser
     * 列族：info
     * 列名：channelId, browser,time,pv,uv,newCount,oldCount
     * rowKey：channelId +time(格式化的日期)
     */
    val tableName:String ="browser"
    val family:String = "info"
    val newCountCol = "newCount"
    val oldCountCol = "oldCount"
    val pvCol = "pv"
    val uvCol = "uv"
    val rowKey: String = value.getChannelId + value.getTimeFormat

    var pv: Long = value.getPv
    var uv: Long = value.getUv
    var newCount: Long = value.getNewCount
    var oldCount: Long = value.getOldCount

    //先查询hbase，如果pv ,uv,newCount,oldCount不为null,需要与现有数据，进行累加操作
    val pvData: String = HBaseUtil.queryByRowKey(tableName, family, pvCol, rowKey)
    val uvData: String = HBaseUtil.queryByRowKey(tableName, family, uvCol, rowKey)
    val newCountData: String = HBaseUtil.queryByRowKey(tableName, family, newCountCol, rowKey)
    val oldCountData: String = HBaseUtil.queryByRowKey(tableName, family, oldCountCol, rowKey)

    //非空判断
    if (StringUtils.isNotBlank(pvData)) {
      pv = pv + pvData.toLong
    }
    if (StringUtils.isNotBlank(uvData)) {
      uv = uv + uvData.toLong
    }
    if (StringUtils.isNotBlank(newCountData)) {
      newCount = newCount + newCountData.toLong
    }
    if (StringUtils.isNotBlank(oldCountData)) {
      oldCount = oldCount + oldCountData.toLong
    }

    //封装map数据，执行多列一次性插入
    var map =Map[String,Any]()
    //列名：channelId, browser,time,pv,uv,newCount,oldCount
    map+=("channelId"->value.getChannelId)
    map+=("browseType"-> value.getBrowserType)
    map+=("time"->value.getTimeFormat)
    map+=(pvCol->pv)
    map+=(uvCol->uv)
    map+=(newCountCol->newCount)
    map+=(oldCountCol->oldCount)

    //执行插入操作
    HBaseUtil.putMapDataByRowKey(tableName,family,map,rowKey)
  }
}
