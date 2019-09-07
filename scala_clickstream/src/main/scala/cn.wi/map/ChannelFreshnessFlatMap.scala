package cn.wi.map

import cn.wi.pojo.{ChannelFreshness, Message, UserBrowse, UserState}
import cn.wi.until.TimeUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelFreshnessFlatMap
 * @Author: xianlawei
 * @Description: 用户新鲜度的自定义转换
 * @date: 2019/9/6 15:29
 */
class ChannelFreshnessFlatMap extends RichFlatMapFunction[Message, ChannelFreshness] {

  //定义格式化模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"

  override def flatMap(value: Message, out: Collector[ChannelFreshness]): Unit = {
    val userBrowse: UserBrowse = value.userBrowse
    val channelId: Long = userBrowse.channelId
    val timeStamp: Long = userBrowse.timeStamp
    val userId: Long = userBrowse.userId

    //获取用户访问状态
    val userState: UserState = UserState.getUserState(timeStamp,userId)
    val isNew: Boolean = userState.isNew
    val firstHour: Boolean = userState.isFirstHour
    val firstDay: Boolean = userState.isFirstDay
    val firstMonth: Boolean = userState.isFirstMonth

    //新建Bean 封装部分数据
    val freshness: ChannelFreshness = new ChannelFreshness
    freshness.setChannelId(channelId)

    //对当前日期进行格式化操作
    val hourTime: String = TimeUtil.parseTime(timeStamp,hour)
    val dayTime: String = TimeUtil.parseTime(timeStamp,day)
    val monthTime: String = TimeUtil.parseTime(timeStamp,month)

    //根据用户访问状态，判断是否是新老用户
    if (isNew) {
      freshness.setNewCount(1L)
    } else {
      freshness.setNewCount(1L)
    }

    //小时维度
    if (firstHour) {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(hourTime)
      out.collect(freshness)
    } else {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(hourTime)
      out.collect(freshness)
    }

    //天维度
    if (firstDay) {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(dayTime)
      out.collect(freshness)
    } else {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(dayTime)
      out.collect(freshness)
    }

    //小时维度
    if (firstMonth) {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(monthTime)
      out.collect(freshness)
    } else {
      freshness.setNewCount(1L)
      freshness.setTimeFormat(monthTime)
      out.collect(freshness)
    }


  }
}
