package org.sunbird.dp.spec

import com.google.gson.Gson
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.util.CBEventsFlattener
import org.sunbird.dp.fixture.CBEventFixture

import java.util
import scala.collection.JavaConverters._

class CBEventsFlattenerUtilTestSpec extends FlatSpec with BeforeAndAfterAll {

  val gson = new Gson()
  val cbEventsFlattenerUtil = new CBEventsFlattener()

  override def beforeAll() {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "CBEventsFlattenerUtil" should "flatten a valid work order event" in {
    val cbEvent = new Event(gson.fromJson(CBEventFixture.SAMPLE_WO_EVENT, new util.LinkedHashMap[String, Any]().getClass))
    val expectedFlattenedEvents = gson.fromJson(CBEventFixture.SAMPLE_WO_EVENT, new util.ArrayList[util.LinkedHashMap[String, Any]]().getClass)
    val events = cbEventsFlattenerUtil.flattenedEvents(cbEvent).toList.asInstanceOf[util.ArrayList[util.LinkedHashMap[String, Any]]]

  }

}