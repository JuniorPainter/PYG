package cn.wi.pojo

import scala.beans.BeanProperty


/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelPvUv
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 13:37
 */
class ChannelPvUv {
  @BeanProperty var channelId: Long = 0L
  @BeanProperty var timeFormat: String = null
  @BeanProperty var Pv: Long = 0L
  @BeanProperty var Uv: Long = 0L

  //get方法
  //  def getChannelId: Long = channelId
  //
  //  def getTimeFormat: String = timeFormat
  //
  //  def getPv: Long = Pv
  //
  //  def getUv: Long = Uv

  //set方法
  //  def setChannelId(value: Long) = {
  //    channelId = value
  //  }
  //
  //  def setTimeFormat(value: String) = {
  //    timeFormat = value
  //  }
  //
  //  def setPv(value: Long) = {
  //    Pv = value
  //  }
  //
  //  def setUv(value: Long) = {
  //    Uv = value
  //  }

}
