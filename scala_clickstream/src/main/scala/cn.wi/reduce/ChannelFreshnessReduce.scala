package cn.wi.reduce

import cn.wi.pojo.ChannelFreshness
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelFreshnessReduce
 * @Author: xianlawei
 * @Description: 用户新鲜度的自定义数据聚合
 * @date: 2019/9/6 15:44
 */
class ChannelFreshnessReduce extends ReduceFunction[ChannelFreshness] {
  override def reduce(value1: ChannelFreshness, value2: ChannelFreshness): ChannelFreshness = {
    val freshness: ChannelFreshness = new ChannelFreshness

    freshness.setChannelId(value1.getChannelId)
    freshness.setNewCount(value1.getNewCount + value2.getNewCount)
    freshness.setOldCount(value1.getOldCount + value2.getOldCount)
    freshness.setTimeFormat(value1.getTimeFormat)

    freshness
  }
}
