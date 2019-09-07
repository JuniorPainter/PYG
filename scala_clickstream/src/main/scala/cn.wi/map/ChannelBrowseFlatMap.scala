package cn.wi.map

import cn.wi.pojo.{ChannelBrowse, Message, UserBrowse, UserState}
import cn.wi.until.TimeUtil
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.util.Collector


/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelBrowseFlatMap
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:40
 */
class ChannelBrowseFlatMap extends RichFlatMapFunction[Message, ChannelBrowse] {
  //定义格式化模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"

  override def flatMap(value1: Message, value2: Collector[ChannelBrowse]): Unit = {
    //取出消息中的部分数据
    val userBrowse: UserBrowse = value1.userBrowse
    val channelID: Long = userBrowse.channelId
    val userID: Long = userBrowse.userId
    val timestamp: Long = userBrowse.timeStamp
    val browserType: String = userBrowse.browserType

    //格式化时间戳
    val hourTime: String = TimeUtil.parseTime(timestamp, hour)
    val dayTime: String = TimeUtil.parseTime(timestamp, day)
    val monthTime: String = TimeUtil.parseTime(timestamp, month)

    //获取用户访问状态
    val userState: UserState = UserState.getUserState(timestamp, userID)
    val isNew: Boolean = userState.isNew
    val firstHour: Boolean = userState.isFirstHour
    val firstDay: Boolean = userState.isFirstDay
    val firstMonth: Boolean = userState.isFirstMonth

    //新建bean，封装部分数据
    val browse = new ChannelBrowse
    browse.setBrowserType(browserType)
    browse.setChannelId(channelID)
    browse.setPv(1L)

    //根据用户的访问状态，封装数据
    if (isNew) {
      browse.setUv(1L)
      browse.setNewCount(1L)
    } else {
      browse.setUv(0L)
      browse.setOldCount(1L)
    }

    //小时维度
    if (firstHour) {
      browse.setNewCount(1L)
      browse.setUv(1L)
      browse.setTimeFormat(hourTime)
      value2.collect(browse)
    } else {
      browse.setOldCount(1L)
      browse.setTimeFormat(hourTime)
      value2.collect(browse)
    }

    //天维度
    if (firstDay) {
      browse.setNewCount(1L)
      browse.setUv(1L)
      browse.setTimeFormat(dayTime)
      value2.collect(browse)
    } else {
      browse.setOldCount(1L)
      browse.setTimeFormat(dayTime)
      value2.collect(browse)
    }

    //月维度
    if (firstMonth) {
      browse.setNewCount(1L)
      browse.setUv(1L)
      browse.setTimeFormat(monthTime)
      value2.collect(browse)
    } else {
      browse.setOldCount(1L)
      browse.setTimeFormat(monthTime)
      value2.collect(browse)
    }
  }
}