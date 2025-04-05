package org.sunbird.dp.eventupdater.functions

import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{PostgresConnect, PostgresConnectionConfig}
import org.sunbird.dp.eventupdater.task.EventUpdaterConfig

class EventUpdaterProcessFunction(config: EventUpdaterConfig,
                                  @transient var postgresConnect: PostgresConnect = null
                                 )(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EventUpdaterProcessFunction])

  override def metricsList(): List[String] = {
    List(config.successCount, config.failedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (postgresConnect == null) {
      postgresConnect = new PostgresConnect(PostgresConnectionConfig(
        user = config.postgresUser,
        password = config.postgresPassword,
        database = config.postgresDb,
        host = config.postgresHost,
        port = config.postgresPort,
        maxConnections = config.postgresMaxConnections
      ))
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: util.Map[String, AnyRef],
                              context: ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val data = event.get("data").asInstanceOf[String]
    if (data != null && !data.isEmpty) {
      try {
        addToPostgres(data)
        metrics.incCounter(config.successCount)
      } catch {
        case e: Exception =>
          logger.error("Failed to process event", e)
          metrics.incCounter(config.failedEventCount)
      }
    } else {
      metrics.incCounter(config.failedEventCount)
    }
  }

  def addToPostgres(data: String): Unit = {
    val postgresQuery = "INSERT INTO " + config.postgresTable + " (data, created_at) VALUES (?, ?)"
    val preparedStatement = postgresConnect.getConnection.prepareStatement(postgresQuery)
    preparedStatement.setString(1, data)
    preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()))
    preparedStatement.executeUpdate()
    preparedStatement.close()
  }
}