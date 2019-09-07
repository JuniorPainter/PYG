package cn.wi.reduce

import cn.wi.pojo.ChannelBrowse
import org.apache.flink.api.common.functions.ReduceFunction


/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelBrowseReduce
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 17:45
 */
class ChannelBrowseReduce extends ReduceFunction[ChannelBrowse] {
  override def reduce(value1: ChannelBrowse, value2: ChannelBrowse): ChannelBrowse = {

    val browse = new ChannelBrowse
    browse.setBrowserType(value1.getBrowserType)
    browse.setChannelId(value1.getChannelId)
    browse.setNewCount(value1.getNewCount + value2.getNewCount)
    browse.setOldCount(value1.getOldCount + value2.getOldCount)
    browse.setPv(value1.getPv + value2.getPv)
    browse.setTimeFormat(value1.getTimeFormat)
    browse.setUv(value1.getUv + value2.getUv)
    browse
  }
}
