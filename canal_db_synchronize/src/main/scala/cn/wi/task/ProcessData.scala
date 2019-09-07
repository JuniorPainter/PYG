package cn.wi.task


import cn.wi.pojo.{Canal, ColumnPair, HBaseOperation}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ArrayBuffer


/**
 * 业务处理类：主要用来封装最终的hbase操作类数据：HbaseOperation
 */

object ProcessData {
  def process(waterCanals: DataStream[Canal]): DataStream[HBaseOperation] = {
    val value: DataStream[HBaseOperation] = waterCanals.flatMap {
      line: Canal =>
        val columnValueList: String = line.columnValueList
        val tableName: String = line.tableName
        val dbName: String = line.dbName
        val eventType: String = line.eventType
        //解析columnValueList
        val pairs: ArrayBuffer[ColumnPair] = ColumnPair.parseJsonArray(columnValueList)
        //hbase表名
        val hbaseTableName: String = dbName + "-" + tableName
        val family: String = "info"
        val rowkey: String = pairs(0).columnValue

        //对pairs做数据转换操作，取列名和列值
        //(3)HbaseOperation(eventType,tableName,family,columnName,columnValue,rowkey)
        eventType match {
          case "INSERT" =>
            pairs.map((line: ColumnPair) => HBaseOperation(eventType, hbaseTableName, family, line.columnName, line.columnValue, rowkey))
          case "UPDATE" =>
            pairs.filter((_: ColumnPair).isValid).map((line: ColumnPair) => HBaseOperation(eventType, hbaseTableName, family, line.columnName, line.columnValue, rowkey))
          case _ =>
            List(HBaseOperation(eventType, hbaseTableName, family, null, null, rowkey))
        }
    }
    value
  }

}
