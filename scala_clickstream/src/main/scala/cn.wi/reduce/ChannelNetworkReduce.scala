package cn.wi.reduce

import cn.wi.pojo.ChannelNetwork
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelNetworkReduce
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:25
 */
class ChannelNetworkReduce extends ReduceFunction[ChannelNetwork] {
  override def reduce(value1: ChannelNetwork, value2: ChannelNetwork): ChannelNetwork = {

    val channelNetwork = new ChannelNetwork

    channelNetwork.setChannelId(value1.getChannelId)
    channelNetwork.setNetWork(value1.getNetWork)
    channelNetwork.setNewCount(value1.getNewCount + value2.getNewCount)
    channelNetwork.setOldCount(value1.getOldCount + value2.getOldCount)
    channelNetwork.setTimeFormat(value1.getTimeFormat)
    channelNetwork
  }
}
