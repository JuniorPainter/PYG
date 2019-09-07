package cn.wi.reduce

import cn.wi.pojo.ChannelRegion
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelRegionReduce
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 16:38
 */
class ChannelRegionReduce extends ReduceFunction[ChannelRegion] {
  override def reduce(value1: ChannelRegion, value2: ChannelRegion): ChannelRegion = {

    val channelRegion = new ChannelRegion

    channelRegion.setChannelId(value1.getChannelId)
    channelRegion.setCity(value1.getCity)
    channelRegion.setCountry(value1.getCountry)
    channelRegion.setProvince(value1.getProvince)
    channelRegion.setTimeFormat(value1.getTimeFormat)

    channelRegion.setNewCount(value1.getNewCount + value2.getNewCount)
    channelRegion.setOldCount(value1.getOldCount + value2.getOldCount)
    channelRegion.setPv(value1.getPv + value2.getPv)
    channelRegion.setUv(value1.getUv + value2.getUv)

    channelRegion
  }
}
