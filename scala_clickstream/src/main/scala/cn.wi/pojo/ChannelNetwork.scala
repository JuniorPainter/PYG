package cn.wi.pojo

import scala.beans.BeanProperty

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelNetwork
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:09
 */
class ChannelNetwork {
  @BeanProperty var channelId: Long = 0L
  @BeanProperty var netWork: String = _
  @BeanProperty var timeFormat: String = _
  @BeanProperty var newCount: Long = 0L
  @BeanProperty var oldCount: Long = 0L
}
