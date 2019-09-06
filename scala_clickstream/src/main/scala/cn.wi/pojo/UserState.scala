package cn.wi.pojo

import cn.wi.until.{HBaseUtil, TimeUtil}
import org.apache.commons.lang3.StringUtils

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: UserState
 * @Author: xianlawei
 * @Description: 用户访问状态
 *               用户状态，如果状态值为true，说明是首次访问，uv=1
 *               如果状态值为false，说明不是首次访问，uv=0
 * @date: 2019/9/6 11:36
 */
case class UserState(
                      /**
                       * 是否是新用户
                       * 在一个小时内是不是第一次访问
                       * 在一天内是不是第一次访问
                       * 在一个月内是不是第一次访问
                       */
                      isNew: Boolean = false,
                      isFirstHour: Boolean = false,
                      isFirstDay: Boolean = false,
                      isFirstMonth: Boolean = false
                    )

object UserState {

  //定义时间格式化模板
  val hour = "yyyyMMddHH"
  val day = "yyyyMMdd"
  val month = "yyyyMM"

  /**
   *
   * @param timeStamp
   * @param userId 作为Hbase的RowKey
   * @return
   */
  def getUserState(timeStamp: Long, userId: Long): UserState = {
    //定义Hbase参数
    val tableName = "userState"
    val family = "info"
    //首次访问时间列
    val firstVisitColumnName = "firstVisitTime"
    //最近一次访问时间列
    val lastVisitColumn = "lastVisitTime"
    val rowKey: String = userId.toString


    //初始化访问状态
    var isNew: Boolean = false
    var isFirstHour: Boolean = false
    var isFirstDay: Boolean = false
    var isFirstMonth: Boolean = false
    //先查询HBase表userState的首次访问时间列，如果为null，说明是第一次访问
    val resultString: String = HBaseUtil.queryByRowKey(tableName, family, firstVisitColumnName, rowKey)

    //判断HBase中数据是否为空  为空表示以前并没有访问过，将初始访问状态为true
    //用户进行第一次访问的时候，会将第一次的访问时间存入首次访问的时间和最近一次访问时间中
    //做出对时间的初始化操作
    if (StringUtils.isBlank(resultString)) {
      isNew = true
      isFirstHour = true
      isFirstDay = true
      isFirstMonth = true

      //把首次访问时间存入HBase
      //首次访问时间
      HBaseUtil.putDataByRowKey(tableName, family, firstVisitColumnName, timeStamp.toString, rowKey)
      //最近一次访问时间
      HBaseUtil.putDataByRowKey(tableName, family, lastVisitColumn, timeStamp.toString, rowKey)

      UserState(isNew, isFirstHour, isFirstDay, isFirstMonth)
    }
    else {
      //说明访问不为空，对最近一次访问列的时间，与当前时间进行大小判断
      //如果首次访问列得时间不为null，查询最近一次的访问时间
      // 首次访问时间不为空，Hbase中存储的时间肯定不为空，不需要进行非空判断
      val lastVisitTimeDate: String = HBaseUtil.queryByRowKey(
        tableName,
        family,
        lastVisitColumn,
        rowKey)

      //timeStamp是用户访问时，携带的时间
      //lastVisitTimeDate 是Hbase中记录的最近一次的访问时间
      //小时
      if (TimeUtil.parseTime(timeStamp, hour).toLong > TimeUtil.parseTime(lastVisitTimeDate.toLong, hour).toLong) {
        isFirstHour = true
      }

      //天
      if (TimeUtil.parseTime(timeStamp, day).toLong > TimeUtil.parseTime(lastVisitTimeDate.toLong, day).toLong) {
        isFirstDay = true
      }

      //月
      if (TimeUtil.parseTime(timeStamp, month).toLong > TimeUtil.parseTime(lastVisitTimeDate.toLong, month).toLong) {
        isFirstMonth = true
      }
    }
    //将HBase中的数据进行更新
    HBaseUtil.putDataByRowKey(tableName, family, lastVisitColumn, timeStamp.toString, rowKey)

    UserState(isNew, isFirstHour, isFirstDay, isFirstMonth)
  }
}