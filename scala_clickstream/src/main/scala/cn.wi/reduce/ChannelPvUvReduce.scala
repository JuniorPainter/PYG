package cn.wi.reduce

import cn.wi.pojo.ChannelPvUv
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelPvUvReduce
 * @Author: xianlawei
 * @Description: 实时的频道的PvUv的自定义聚合方式  将两个ChannelPvUv对象的各个参数进行聚合
 * @date: 2019/9/6 14:04
 */
class ChannelPvUvReduce extends ReduceFunction[ChannelPvUv] {
  override def reduce(value1: ChannelPvUv, value2: ChannelPvUv): ChannelPvUv = {
    //将两个ChannelPvUv对象的各个参数进行聚合
    val channelPvUv: ChannelPvUv = new ChannelPvUv

    channelPvUv.setTimeFormat(value1.getTimeFormat)

    channelPvUv.setChannelId(value2.getChannelId)

    channelPvUv.setUv(value1.getUv + value2.getUv)

    channelPvUv.setPv(value2.getPv + value2.getPv)

    channelPvUv
  }
}
