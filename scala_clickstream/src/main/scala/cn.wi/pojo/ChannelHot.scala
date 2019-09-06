package cn.wi.pojo

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: ChannelHot
 * @Author: xianlawei
 * @Description: 实时频道热点统计的Bean对象
 * @date: 2019/9/6 10:38
 */
case class ChannelHot(
                       count: Int = 0,
                       channelId: Long = 0L,
                       timeStamp: Long = 0
                     )
