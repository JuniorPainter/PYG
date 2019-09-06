package cn.wi.until

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
 * @ProjectName: Flink_PYG 
 * @ClassName: TimeUtil
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/5 9:37
 */
object TimeUtil {
  /**
   *
   * @param timeStamp Long类型的时间：包含时分秒
   * @param format 格式化模板
   * @return 字符串格式的时间
   */
  def parseTime(timeStamp: Long, format: String): String = {
    val date: Date = new Date(timeStamp)
    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance(format)
    val string: String = fastDateFormat.format(date)
    string
  }
}
