import java.util.Properties

import cn.wi.config.GlobalConfig
import cn.wi.pojo.{Message, UserBrowse}
import cn.wi.task.{ChannelHotTask, ChannelPvUvTask}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: AppStream
 * @Author: xianlawei
 * @Description: main方法 入口程序
 * @date: 2019/9/5 20:06
 */
object AppStream {
  /**
   * 1. 获取流处理执行环境
   * 2. 设置时间时间(提取的是消息源本身的时间)
   * 3. 设置检查点
   * 4. 整合kafka
   * 5. 数据转换
   * 6. 设置水位线
   * 7. 任务执行
   * 8. 触发执行
   */
  def main(args: Array[String]): Unit = {
    // 获取流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    //设置事件时间(提取的是消息本身的时间)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置检查点保存目录
    environment.setStateBackend(new FsStateBackend("hdfs://node01:8020/PYG/checkpoint/"))

    //周期性触发时间
    environment.enableCheckpointing(6000)
    //强一致性
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //超时时间一分钟
    environment.getCheckpointConfig.setCheckpointTimeout(60000)
    //检查点制作失败，任务继续运行
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //最大线程数
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //任务取消，保留检查点，手动删除检查点
    //DELETE_ON_CANCELLATION 任务失败删除检查点  生产环境绝对不能用
    environment.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig
        .ExternalizedCheckpointCleanup
        .RETAIN_ON_CANCELLATION
    )

    //整合kafka
    val properties: Properties = new Properties()
    //broker地址
    properties.setProperty("bootstrap.servers", GlobalConfig.kafkaBroker)
    //Zookeeper地址
    properties.setProperty("zookeeper.connect", GlobalConfig.kafkaZookeeper)
    //消费组
    properties.setProperty("group.id", GlobalConfig.kafkaGroupId)

    val kafkaSource: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      GlobalConfig.kafkaTopic,
      new SimpleStringSchema(),
      properties
    )

    //添加Source
    val dataDS: DataStream[String] = environment.addSource(kafkaSource)

    val messageDS: DataStream[Message] = dataDS.map((line: String) => {
      val json: JSONObject = JSON.parseObject(line)
      val count: Int = json.getIntValue("count")
      val message: String = json.getString("message")
      val timeStamp: Long = json.getLong("timeStamp")
      val browse: UserBrowse = UserBrowse.parseString(message)
      Message(count, browse, timeStamp)
    })

    //设置水位线(周期性)
    val waterTimeDS: DataStream[Message] = messageDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {
      val delayTime: Long = 2000L
      var currentTimeStamp: Long = 0L

      //再获取水位线
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - delayTime)
      }

      //先抽取时间  上面必须设置EventTime
      override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {
        val timeStamp: Long = element.userBrowse.timeStamp
        //谁大取谁，保证时间轴，一直往前走
        currentTimeStamp = Math.max(currentTimeStamp, timeStamp)
        timeStamp
      }
    })

    //任务执行
    /**
     * 实时频道的热点统计
     * 实时频道的PvUv分析
     * 实时频道的用户新鲜度
     * 实时频道的地域分析
     * 实时频道的网络类型分析
     * 实时频道的浏览器类型分析
     */

    // 实时频道热点统计
    //ChannelHotTask.process(waterTimeDS)

    //实时频道的PVUv分析
    ChannelPvUvTask.process(waterTimeDS)

    //触发执行
    environment.execute()
  }
}
