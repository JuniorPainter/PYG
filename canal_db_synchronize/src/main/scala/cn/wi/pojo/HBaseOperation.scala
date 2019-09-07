package cn.wi.pojo

/**
  * @Date 2019/9/5
  */
case class HBaseOperation(
                         var eventType:String,
                         var tableName:String,
                         var family:String,
                         var columnName:String,
                         var columnValue:String,
                         var rowKey:String
                         )
