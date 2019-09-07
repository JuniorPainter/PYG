package cn.wi.pojo

import com.alibaba.fastjson.{JSON, JSONObject}


/**
 * @ProjectName: Flink_PYG 
 * @ClassName: Canal
 * @Author: xianlawei
 * @Description:
 * {
 * "emptyCount": 1,
 * "logFileName": "mysql-bin.000001",
 * "dbName": "test",
 * "logFileOffset": 442,
 * "eventType": "INSERT",
 * "columnValueList": [

 * {
 * "columnName": "age",
 * "columnValue": "1",
 * "isValid": true
 * }
 * ],
 * "tableName": "test",
 * "timestamp": 1567774054000
 * }
 * @date: 2019/9/6 21:38
 */
case class Canal(
                  var columnValueList: String,
                  var dbName: String,
                  var emptyCount: String,
                  //更新类型
                  var eventType: String,
                  var logFileName: String,
                  var logFileOffset: String,
                  var tableName: String,
                  var timestamp: String
                )

object Canal {
  def parseString(string: String): Canal = {
    val json: JSONObject = JSON.parseObject(string)
    Canal(
      json.getString("columnValueList"),
      json.getString("dbName"),
      json.getString("emptyCount"),
      json.getString("eventType"),
      json.getString("logFileName"),
      json.getString("logFileOffset"),
      json.getString("tableName"),
      json.getString("timestamp")
    )
  }
}