package org.sunbird.dp.eventupdater.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.dp.core.job.BaseJobConfig
import org.apache.flink.streaming.api.scala._

class EventUpdaterConfig(override val config: Config) extends BaseJobConfig(config, jobName = "EventUpdaterJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val jobParallelism: Int = config.getInt("task.job.parallelism")

  // PostgreSQL Configuration
  val postgresUser: String = config.getString("postgres.user")
  val postgresPassword: String = config.getString("postgres.password")
  val postgresTable: String = config.getString("postgres.table")
  val postgresDb: String = config.getString("postgres.database")
  val postgresHost: String = config.getString("postgres.host")
  val postgresPort: Int = config.getInt("postgres.port")
  val postgresMaxConnections: Int = config.getInt("postgres.maxConnections")

  // Metrics
  val successCount = "success-event-count"
  val failedEventCount = "failed-event-count"
}