package cn.wi.pojo

import scala.beans.BeanProperty

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelBrowse
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:39
 */
class ChannelBrowse {
  @BeanProperty var channelId: Long = 0L
  @BeanProperty var browserType: String = _
  @BeanProperty var timeFormat: String = _
  @BeanProperty var pv: Long = 0L
  @BeanProperty var uv: Long = 0L
  @BeanProperty var newCount: Long = 0L
  @BeanProperty var oldCount: Long = 0L
}
