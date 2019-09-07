package cn.wi.map

import cn.wi.pojo.{ChannelNetwork, Message, UserBrowse, UserState}
import cn.wi.until.TimeUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelNetworkFlatMap
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:08
 */
class ChannelNetworkFlatMap extends RichFlatMapFunction[Message, ChannelNetwork] {
  //定义格式化日期模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"

  override def flatMap(value: Message, out: Collector[ChannelNetwork]): Unit = {
    val userBrowse: UserBrowse = value.userBrowse

    val channelId: Long = userBrowse.channelId
    val userId: Long = userBrowse.userId
    val timeStamp: Long = userBrowse.timeStamp
    val network: String = userBrowse.network

    //获取用户访问状态
    val userState: UserState = UserState.getUserState(timeStamp, userId)
    val isNew: Boolean = userState.isNew
    val firstDay: Boolean = userState.isFirstDay
    val firstHour: Boolean = userState.isFirstHour
    val firstMonth: Boolean = userState.isFirstMonth

    //对当前时间戳进行格式化操作
    val hourTime: String = TimeUtil.parseTime(timeStamp, hour)
    val dayTime: String = TimeUtil.parseTime(timeStamp, day)
    val monthTime: String = TimeUtil.parseTime(timeStamp, month)

    //新建Bean，封装部分数据
    val channelNetwork: ChannelNetwork = new ChannelNetwork
    channelNetwork.setChannelId(channelId)
    channelNetwork.setNetWork(network)

    //根据用户状态，封装数据
    if (isNew) {
      channelNetwork.setNewCount(1L)
    } else {
      channelNetwork.setOldCount(1L)
    }

    //小时维度
    if (firstHour) {
      channelNetwork.setNewCount(1L)
      channelNetwork.setTimeFormat(hourTime)
      out.collect(channelNetwork)
    } else {
      channelNetwork.setOldCount(1L)
      channelNetwork.setTimeFormat(hourTime)
      out.collect(channelNetwork)
    }
    //天维度
    if (firstDay) {
      channelNetwork.setNewCount(1L)
      channelNetwork.setTimeFormat(dayTime)
    } else {
      channelNetwork.setOldCount(1L)
      channelNetwork.setTimeFormat(dayTime)
      out.collect(channelNetwork)
    }

    //月维度
    if (firstMonth) {
      channelNetwork.setNewCount(1L)
      channelNetwork.setTimeFormat(monthTime)
    } else {
      channelNetwork.setOldCount(1L)
      channelNetwork.setTimeFormat(monthTime)
      out.collect(channelNetwork)
    }
  }
}
