package cn.wi.pojo

import scala.beans.BeanProperty

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelFreshness
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 15:30
 */
class ChannelFreshness {
  @BeanProperty var channelId: Long = 0L
  @BeanProperty var timeFormat: String = null
  @BeanProperty var newCount: Long = 0L
  @BeanProperty var oldCount: Long = 0L
}
