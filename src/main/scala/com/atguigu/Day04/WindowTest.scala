package com.atguigu.Day04

import com.atguigu.Day02.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream=env.socketTextStream("hadoop105",9999)
    val datastream=inputStream.map(
      data => {
        val dataArray=data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = (element.temperature *100L).toLong
      })
    val resultstream=datastream
      .keyBy("id")
      .timeWindow(Time.seconds(15),Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late"))
  }
}

//自定义一个周期性生产watermark的Assigner
class MyWMAssigner(lateness:Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  var maxTs:Long=Long.MinValue+lateness
  override def getCurrentWatermark: Watermark = new Watermark(maxTs-lateness)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs=maxTs.max(element.timestamp*1000L)
    element.timestamp*1000L
  }
}

//自定义断点式生成watermark的Assigner
class MyWMAssinger2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val lateness:Long=1000L
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id =="sensor_id"){
      new Watermark(extractedTimestamp-lateness)
    }else
      null
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long =
    element.timestamp*1000L
}
