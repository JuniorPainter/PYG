package cn.wi.map

import cn.wi.pojo.{ChannelPvUv, Message, UserBrowse, UserState}
import cn.wi.until.TimeUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelPvUvFlatMap
 * @Author: xianlawei
 * @Description:  实时的频道的PvUv的自定义数据转换
 * @date: 2019/9/6 13:36
 */
class ChannelPvUvFlatMap extends RichFlatMapFunction[Message, ChannelPvUv] {
  //定义格式化模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"


  override def flatMap(value: Message, out: Collector[ChannelPvUv]): Unit = {
    val browse: UserBrowse = value.userBrowse
    val channelId: Long = browse.channelId
    val timeStamp: Long = browse.timeStamp
    val userId: Long = browse.userId

    //根据userId和tomeStamp获取访问用户时间状态
    val userState: UserState = UserState.getUserState(timeStamp, userId)
    //获取访问状态
    val isNew: Boolean = userState.isNew
    val firstHour: Boolean = userState.isFirstHour
    val firstDay: Boolean = userState.isFirstDay
    val firstMonth: Boolean = userState.isFirstMonth

    //格式化日期   timeStamp消息自身携带的日期
    val hourTime: String = TimeUtil.parseTime(timeStamp, hour)
    val dayTime: String = TimeUtil.parseTime(timeStamp, day)
    val monthTime: String = TimeUtil.parseTime(timeStamp, month)

    //新建Bean封装部分数据
    val channelPvUv = new ChannelPvUv
    channelPvUv.setPv(1L)
    channelPvUv.setChannelId(channelId)

    //根据用户访问的时间状态，进行设置值，UV
    if (isNew) {
      channelPvUv.setUv(1L)
    } else {
      channelPvUv.setUv(0L)
    }


    //小时维度
    if (firstHour) {
      channelPvUv.setUv(1)
      channelPvUv.setTimeFormat(hourTime)
      out.collect(channelPvUv)
    } else {
      channelPvUv.setUv(0L)
      channelPvUv.setTimeFormat(hourTime)
      out.collect(channelPvUv)
    }

    //天维度
    if (firstDay) {
      channelPvUv.setUv(1)
      channelPvUv.setTimeFormat(dayTime)
      out.collect(channelPvUv)
    } else {
      channelPvUv.setUv(0L)
      channelPvUv.setTimeFormat(dayTime)
      out.collect(channelPvUv)

    }

    //月维度
    if (firstMonth) {
      channelPvUv.setUv(1)
      channelPvUv.setTimeFormat(monthTime)
      out.collect(channelPvUv)
    } else {
      channelPvUv.setUv(0L)
      channelPvUv.setTimeFormat(monthTime)
      out.collect(channelPvUv)
    }

  }
}
