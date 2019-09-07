package cn.wi.sink

import cn.wi.pojo.ChannelRegion
import cn.wi.until.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelRegionSink
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 16:43
 */
class ChannelRegionSink extends RichSinkFunction[ChannelRegion] {
  //将数据插入到HBase中
  override def invoke(value: ChannelRegion): Unit = {
    /**
     * 数据落地：channelId, country,province,city,time,pv,uv,newCount,oldCount
     * 设计hbase表：
     * 表名：region
     * 列族：info
     * 列名：channelId, country,province,city,time,pv,uv,newCount,oldCount
     * rowKey：channelId +time(格式化的日期)
     */

    val tableName: String = "region"
    val family: String = "info"
    val pvColumn: String = "pv"
    val uvColumn: String = "uv"
    val newCountColumn: String = "newCount"
    val oldCountColumn: String = "oldCount"
    val rowKey: String = value.getChannelId + value.getTimeFormat

    var pv: Long = value.getPv
    var uv: Long = value.getUv
    var newCount: Long = value.getNewCount
    var oldCount: Long = value.getOldCount

    //先查询结果Hbase，如果Pv、Uv、newCount,oldCount不为null,需要与现有数据，进行累加操作
    val pvData: String = HBaseUtil.queryByRowKey(tableName, family, pvColumn, rowKey)
    val uvData: String = HBaseUtil.queryByRowKey(tableName, family, uvColumn, rowKey)

    val newCountData: String = HBaseUtil.queryByRowKey(tableName, family, newCountColumn, rowKey)
    val oldCountData: String = HBaseUtil.queryByRowKey(tableName, family, oldCountColumn, rowKey)

    //非空判断
    if (StringUtils.isNoneBlank(pvData)) {
      pv = pv + pvData.toLong
    }

    if (StringUtils.isNoneBlank(uvData)) {
      uv = uv + uvData.toLong
    }

    if (StringUtils.isNoneBlank(newCountData)) {
      newCount = newCount + newCountData.toLong
    }

    if (StringUtils.isNoneBlank(oldCountData)) {
      oldCount = oldCount + oldCountData.toLong
    }

    //封装map数，一次性插入HBase(多列数据)
    var map: Map[String, Any] = Map[String, Any]()

    //列名：channelId, country,province,city,time,pv,uv,newCount,oldCount
    map += ("channelId" -> value.getChannelId)
    map += ("country" -> value.getCountry)
    map += ("province" -> value.getProvince)
    map += ("city" -> value.getCity)
    map += ("time" -> value.getTimeFormat)
    map += (pvColumn -> pv)
    map += (uvColumn -> uv)
    map += (newCountColumn -> newCount)
    map += (oldCountColumn -> oldCount)

    //执行插入操作
    HBaseUtil.putMapDataByRowKey(tableName, family, map, rowKey)
  }

}
