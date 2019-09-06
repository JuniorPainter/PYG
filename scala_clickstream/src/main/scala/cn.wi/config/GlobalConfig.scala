package cn.wi.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @ProjectName: Flink_PYG 
 * @ClassName: GlobalConfig
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/5 9:41
 */
object GlobalConfig {
  //  会自动加载resource下面的文件
  private val config: Config = ConfigFactory.load()

  //  kafka配置
  val kafkaBroker: String = config.getString("bootstrap.servers")
  val kafkaZookeeper: String = config.getString("zookeeper.connect")
  val kafkaTopic: String = config.getString("input.topic")
  val kafkaGroupId: String = config.getString("group.id")

  //  hbase的配置
  val hbaseZookeeper: String = config.getString("hbase.zookeeper.quorum")
  val hbaseRpc: String = config.getString("hbase.rpc.timeout")
  val hbaseOperationTimeout: String = config.getString("hbase.client.operation.timeout")
  val hbaseScanTimeOut: String = config.getString("hbase.client.scanner.timeout.period")
}
