package cn.wi.map

import cn.wi.pojo.{ChannelRegion, Message, UserBrowse, UserState}
import cn.wi.until.TimeUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelRegionFlatMap
 * @Author: xianlawei
 * @Description: 实时频道分析的自定义flatMap方法
 * @date: 2019/9/6 16:15
 */
class ChannelRegionFlatMap extends RichFlatMapFunction[Message, ChannelRegion] {
  //定义格式化模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"

  override def flatMap(value: Message, out: Collector[ChannelRegion]): Unit = {
    //取出消息中的部分数据
    val userBrowse: UserBrowse = value.userBrowse

    val channelId: Long = userBrowse.channelId
    val userId: Long = userBrowse.userId
    val timeStamp: Long = userBrowse.timeStamp
    val country: String = userBrowse.country
    val province: String = userBrowse.province
    val city: String = userBrowse.city

    //格式化时间戳
    val hourTime: String = TimeUtil.parseTime(timeStamp, hour)
    val dayTime: String = TimeUtil.parseTime(timeStamp, day)
    val monthTime: String = TimeUtil.parseTime(timeStamp, month)

    //获取用户访问时间状态
    val userState: UserState = UserState.getUserState(timeStamp, userId)

    val isNew: Boolean = userState.isNew
    val firstHour: Boolean = userState.isFirstHour
    val firstDay: Boolean = userState.isFirstDay
    val firstMonth: Boolean = userState.isFirstMonth

    //新建Bean对象封装部分数据
    val channelRegion: ChannelRegion = new ChannelRegion
    channelRegion.setChannelId(channelId)
    channelRegion.setCity(city)
    channelRegion.setProvince(province)
    channelRegion.setCountry(country)
    channelRegion.setPv(1L)

    if (isNew) {
      channelRegion.setNewCount(1L)
      channelRegion.setUv(1L)
    } else {
      channelRegion.setNewCount(1L)
      channelRegion.setUv(0L)
    }
    //小时维度
    if (firstHour) {
      channelRegion.setUv(1L)
      channelRegion.setNewCount(1L)
      channelRegion.setTimeFormat(hourTime)
      out.collect(channelRegion)
    } else {
      channelRegion.setUv(0L)
      channelRegion.setOldCount(1L)
      channelRegion.setTimeFormat(hourTime)
      out.collect(channelRegion)
    }

    //天维度
    if (firstDay) {
      channelRegion.setUv(1L)
      channelRegion.setNewCount(1L)
      channelRegion.setTimeFormat(dayTime)
      out.collect(channelRegion)
    } else {
      channelRegion.setUv(0L)
      channelRegion.setOldCount(1L)
      channelRegion.setTimeFormat(dayTime)
      out.collect(channelRegion)
    }

    //月维度
    if (firstMonth) {
      channelRegion.setUv(1L)
      channelRegion.setNewCount(1L)
      channelRegion.setTimeFormat(monthTime)
      out.collect(channelRegion)
    } else {
      channelRegion.setUv(0L)
      channelRegion.setOldCount(1L)
      channelRegion.setTimeFormat(monthTime)
      out.collect(channelRegion)
    }
  }
}
