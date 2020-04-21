package com.atguigu.Day03.sinkTest

import java.util

import com.atguigu.Day02.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object esSink {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputstream=env.readTextFile("D:\\scalacode\\Filnk\\src\\main\\resources\\sensor.txt")
    val dataStream=inputstream.map(
      data =>{
        val dataArray=data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
    val HttpHosts=new util.ArrayList[HttpHost]()
    HttpHosts.add(new HttpHost("hadoop105",9200))
    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] =new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //写入es 的数据
        val datasource=new util.HashMap[String,String]()
        datasource.put("sensor_id",t.id)
        datasource.put("temp",t.temperature.toString)
        datasource.put("ts",t.timestamp.toString)
        val indexRequest=Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .id(t.id + "-" +System.currentTimeMillis())
          .source(datasource)
        requestIndexer.add(indexRequest)
        println(t +"saved successfully")

      }
    }
    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](HttpHosts,esSinkFunc).build())
    env.execute("es sink test")
  }
}
