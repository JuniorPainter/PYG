package cn.wi.pojo

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer

/**
 * @Date 2019/9/5
 */
case class ColumnPair(
                       var columnName: String,
                       var columnValue: String,
                       var isValid: Boolean
                     )


object ColumnPair {

  def parseJsonArray(str: String): ArrayBuffer[ColumnPair] = {

    val jsonArray: JSONArray = JSON.parseArray(str)
    //新建数组，封装jsonArray数据
    val pairs: ArrayBuffer[ColumnPair] = new ArrayBuffer[ColumnPair]()
    for (line <- 0 until jsonArray.size()) {
      //解析jsonArray内的一条条json字符串
      val json: JSONObject = jsonArray.getJSONObject(line)
      val pair: ColumnPair = ColumnPair(
        json.getString("columnName"),
        json.getString("columnValue"),
        json.getBoolean("isValid")
      )
      pairs += pair
    }
    pairs
  }


}
