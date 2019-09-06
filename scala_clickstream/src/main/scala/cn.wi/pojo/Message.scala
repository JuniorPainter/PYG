package cn.wi.pojo

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: Message
 * @Author: xianlawei
 * @Description:
 *  数据格式
 * {
 * "count": 1,
 * "message": "{\"browserType\":\"火狐\",\"categoryId\":14,\"channelId\":7,\"city\":\"America\",\"country\":\"America\",\"entryTime\":1544630460000,\"leaveTime\":1544634060000,\"network\":\"联通\",\"produceId\":11,\"province\":\"china\",\"source\":\"360搜索跳转\",\"timestamp\":1567689228665,\"userId\":7}",
 * "timeStamp": 1567689229717
 * }
 *
 * @date: 2019/9/5 20:52
 */
case class Message(count:Int,userBrowse: UserBrowse,timeStamp: Long)
