package cn.wi

import java.util.Properties

import cn.wi.config.GlobalConfig
import cn.wi.pojo.{Canal, HBaseOperation}
import cn.wi.task.ProcessData
import cn.wi.util.HBaseUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: AppSynchronize
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/6 21:16
 */
object AppSynchronize {
  def main(args: Array[String]): Unit = {
    /**
     * 获取执行环境
     * 设置处理时间
     * 设置检查点
     * 整合kafka(配置类)
     * 数据转换
     *        1.Canal
     *        2.ColumnValuePair
     *        3.HbaseOperation
     * 不需要设置水位线，此处是实时时间
     * 数据处理
     * 触发执行
     */

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    environment.setStateBackend(new FsStateBackend("hdfs://node01:8020/PYG/sync-checkpoint//"))
    environment.enableCheckpointing(6000)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(60000)
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    environment.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig
        .ExternalizedCheckpointCleanup
        .RETAIN_ON_CANCELLATION
    )

    //整合kafka
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", GlobalConfig.kafkaBroker)
    properties.setProperty("zookeeper.connect", GlobalConfig.kafkaZookeeper)
    properties.setProperty("group.id", GlobalConfig.kafkaGroupId)

    val kafkaSource: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      GlobalConfig.kafkaTopic,
      new SimpleStringSchema(),
      properties)

    val source: DataStream[String] = environment.addSource(kafkaSource)

    val canalDS: DataStream[Canal] = source.map((line: String) => {
      val canal: Canal = Canal.parseString(line)
      canal
    })

    //设置水位线
    val waterDS: DataStream[Canal] = canalDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Canal] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      override def extractTimestamp(element: Canal, previousElementTimestamp: Long): Long = {
        System.currentTimeMillis()
      }
    })

    val value: DataStream[HBaseOperation] = ProcessData.process(waterDS)
    //数据处理
    value.map((line: HBaseOperation) =>{
      //执行hbase的插入操作
      line.eventType match {

        case "DELETE"=>
          HBaseUtil.deleteByRowKey(line.tableName,line.family,line.rowKey)
        case _ =>
          HBaseUtil.putDataByRowKey(line.tableName,line.family,line.columnName,line.columnValue,line.rowKey)
      }
    })

    environment.execute()

  }
}
