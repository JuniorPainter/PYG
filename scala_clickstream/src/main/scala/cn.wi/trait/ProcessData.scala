package cn.wi.`trait`

import cn.wi.pojo.Message
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ProcessData
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 10:13
 */
trait ProcessData {
  def process(waterTimeDS: DataStream[Message])
}
