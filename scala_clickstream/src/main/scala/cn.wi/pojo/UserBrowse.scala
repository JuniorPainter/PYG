package cn.wi.pojo

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: UserBrowse
 * @Author: xianlawei
 * @Description: 日志信息中的message信息内容的UserBrowser的实体类
 *   "message": "{
 *   \"browserType\":\"火狐\",\"categoryId\":14,\"channelId\":7,\"city\":\"America\",\"country\":\"America\",\"entryTime\":1544630460000,\"leaveTime\":1544634060000,\"network\":\"联通\",\"produceId\":11,\"province\":\"china\",\"source\":\"360搜索跳转\",\"timestamp\":1567689228665,\"userId\":7}",
 * @date: 2019/9/5 20:33
 */
case class UserBrowse(
                       /**
                        * 频道ID
                        */
                       val channelId: Long = 0L,

                       /**
                        * 产品的类别ID
                        */
                       val categoryId: Long = 0L,

                       /**
                        * 产品ID
                        */
                       val produceId: Long = 0L,

                       /**
                        * 国家
                        */
                       val country: String = null,

                       /**
                        * 省份
                        */
                       val province: String = null,

                       /**
                        * 城市
                        */
                       val city: String = null,

                       /**
                        * 网络方式
                        */
                       val network: String = null,

                       /**
                        * 来源方式
                        */
                       val source: String = null,

                       /**
                        * 浏览器类型
                        */
                       val browserType: String = null,

                       /**
                        * 进入网站时间
                        */
                       val entryTime: Long = 0L,

                       /**
                        * 离开网站时间
                        */
                       val leaveTime: Long = 0L,

                       /**
                        * 用户的ID
                        */
                       val userId: Long = 0L,

                       /**
                        * 日志产生时间
                        */
                       val timeStamp: Long = 0L
                     )

object UserBrowse {
  def parseString(sting: String): UserBrowse = {
    //字符串转JSON封装成UserBrowse对象
    val json: JSONObject = JSON.parseObject(sting)
    UserBrowse(
      json.getLong("channelId"),
      json.getLong("categoryId"),
      json.getLong("produceId"),
      json.getString("country"),
      json.getString("province"),
      json.getString("city"),
      json.getString("network"),
      json.getString("source"),
      json.getString("browserType"),
      json.getLong("entryTime"),
      json.getLong("leaveTime"),
      json.getLong("userId"),
      //此处与UserBrowse对象中的名字要一致
      json.getLong("timeStamp")
    )

  }
}