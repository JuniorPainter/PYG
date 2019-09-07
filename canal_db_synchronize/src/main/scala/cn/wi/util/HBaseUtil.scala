package cn.wi.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: HBaseUtil
 * @Author: xianlawei
 * @Description:
 *                对hbase进行增删改查
 *                1.初始化
 *                2.根据rowKey查询数据
 *                3.根据rowKey插入单列数据
 *                4.根据rowKey插入多列数据
 *                5.根据rowKey删除数据
 * @date: 2019/9/5 9:17
 */
object HBaseUtil {
  //获取设置对象
  private val configuration: Configuration = HBaseConfiguration.create()


  //获取connection连接
  private val connection: Connection = ConnectionFactory.createConnection(configuration)

  //获取admin
  private val admin: Admin = connection.getAdmin

  /**
   *
   * @param tableName 表名
   * @param family    列族
   * @return 表
   *         初始化表
   */
  def initTable(tableName: String, family: String): Table = {
    val hbaseTableName: TableName = TableName.valueOf(tableName)

    //先判断表是否存在，存在就直接获取，不存在，需要新建表
    if (!admin.tableExists(hbaseTableName)) {
      //构建表描述器
      val tableDescriptor: HTableDescriptor = new HTableDescriptor(hbaseTableName)

      //构建列族描述器
      val columnDescriptor: HColumnDescriptor = new HColumnDescriptor(family)
      tableDescriptor.addFamily(columnDescriptor)

      //创建表
      admin.createTable(tableDescriptor)
    }
    val table: Table = connection.getTable(hbaseTableName)
    table
  }

  /**
   * 根据RoeKey查询数据
   *
   * @param tableName  表名
   * @param family     列族
   * @param columnName 列名
   * @param rowKey     rowKey
   * @return
   *
   */
  def queryByRowKey(tableName: String, family: String, columnName: String, rowKey: String): String = {
    //初始化表
    val table: Table = initTable(tableName, family)
    var string = ""
    try {
      val get: Get = new Get(rowKey.getBytes())
      val result: Result = table.get(get)

      val bytes: Array[Byte] = result.getValue(family.getBytes(), columnName.getBytes())

      if (bytes != null && bytes.length > 0) {
        string = new String(bytes)
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
    string
  }

  /**
   * 根据RowKey插入单列数据
   *
   * @param tableName   表名
   * @param family      列组装
   * @param columnName  列名
   * @param columnValue 列值
   * @param rowKey      RowKey
   */
  def putDataByRowKey(tableName: String, family: String, columnName: String, columnValue: String, rowKey: String): Unit = {
    //初始化表
    val table: Table = initTable(tableName, family)
    try {
      val put: Put = new Put(rowKey.getBytes())
      put.addColumn(family.getBytes(), columnName.getBytes(), columnValue.getBytes())
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }


  }

  /**
   * 根据RowKey插入多列数据
   *
   * @param tableName 表名
   * @param family    列族
   * @param map       K是列名 V是列值
   * @param rowKey    rowKey
   */
  def putMapDataByRowKey(tableName: String, family: String, map: Map[String, Any], rowKey: String): Unit = {
    //初始化表
    val table: Table = initTable(tableName, family)
    try {
      val put: Put = new Put(rowKey.getBytes())
      for ((x, y) <- map) {
        //x 是列名   y 是列值 是Any类型 所以要先转成字符串 再转成字节
        put.addColumn(family.getBytes(), x.getBytes(), y.toString.getBytes())
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 根据rowKey删除数据
   *
   * @param tableName 表名
   * @param family    列族
   * @param rowKey    rowKey
   */
  def deleteByRowKey(tableName: String, family: String, rowKey: String): Unit = {
    //初始化表
    val table: Table = initTable(tableName, family)
    try {
      val delete: Delete = new Delete(rowKey.getBytes())
      table.delete(delete)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 增删改查测试
   */

  def main(args: Array[String]): Unit = {
    //插入单列数据
//    putDataByRowKey(
//      "pyg",
//      "info",
//      "column",
//      "11",
//      "001")

    //查询001的数据
        val result: String = queryByRowKey(
          "pyg",
          "info",
          "column",
          "001")
        println(result)

    //插入多列数据
    //    val map: Map[String, Int] = Map("a" -> 1, "b" -> 2)
    //    putMapDataByRowKey(
    //      "pyg",
    //      "info",
    //      map,
    //      "002")

    //根据RowKey删除数据
        deleteByRowKey(
          "pyg",
          "info",
          "001")
  }

}