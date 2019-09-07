package cn.wi.pojo

import scala.beans.BeanProperty

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelRegion
 * @Author: xianlawei
 * @Description: 实时频道分析的Bean对象
 * @date: 2019/9/6 16:16
 */
class ChannelRegion {
  @BeanProperty var channelId: Long = 0L
  @BeanProperty var country: String = _
  @BeanProperty var province: String = _
  @BeanProperty var city: String = _
  @BeanProperty var timeFormat: String = _
  @BeanProperty var Pv: Long = 0L
  @BeanProperty var Uv: Long = 0L
  @BeanProperty var newCount: Long = 0L
  @BeanProperty var oldCount: Long = 0L

}
