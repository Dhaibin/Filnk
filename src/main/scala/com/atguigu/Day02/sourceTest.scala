package com.atguigu.Day02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random
case class SensorReading( id: String, timestamp: Long, temperature: Double )
object sourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1: DataStream[SensorReading] = env.fromCollection(
      ( List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1),
        SensorReading("sensor_1", 1547718207, 37.2),
        SensorReading("sensor_1", 1547718212, 33.5),
        SensorReading("sensor_1", 1547718215, 38.1)
      ) ))
//
    val stream2=env.readTextFile("D:\\scalacode\\Filnk\\src\\main\\resources\\sensor.txt")
    val props=new Properties()
    props.setProperty("bootstrap.servers", "hadoop105:9092,hadoop104:9092,hadoop106:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    val stream3=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),props))

    val stream4=env.addSource(new MySensorSource())





    stream3.print()
    env.execute()
  }
}
class MySensorSource() extends SourceFunction[SensorReading]{
  var running=true
  override def run(cx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand=new Random()
    var curTemps=1.to(10).map(
      i =>("sensor_"+i,60+rand.nextGaussian()*20)
    )
    while(running){
      curTemps=curTemps.map(
        data =>(data._1,data._2+rand.nextGaussian() )
      )
      val curTs=System.currentTimeMillis()
      curTemps.foreach(
        data=>cx.collect(SensorReading(data._1,curTs,data._2))
      )
    }
  }

  override def cancel(): Unit = running=false
}