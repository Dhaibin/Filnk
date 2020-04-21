package com.atguigu.Day03.sinkTest

import java.util.Properties

import com.atguigu.Day02.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val props=new Properties()
    props.setProperty("bootstrap.servers", "hadoop105:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),props))

    val dataStream: DataStream[String] =inputStream
      .map(data =>{
        val dataArray=data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop105:9092","sinkTest",new SimpleStringSchema()))
    env.execute("kafka sink test")
  }
}
