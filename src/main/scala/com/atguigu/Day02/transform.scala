package com.atguigu.Day02

import org.apache.flink.streaming.api.scala._

object transform {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1: DataStream[String] = env.readTextFile("D:\\scalacode\\Filnk\\src\\main\\resources\\sensor.txt")
    //基本转换操作
    val dataStream=stream1.map(
      data =>{
        val dataArray=data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      }
    )
    //分组滚动聚合,聚合出每个sensor的最大时间戳和最小温度值
    val arrStream=dataStream
        .keyBy(0)
        .reduce((curr ,newdata) =>
        SensorReading(curr.id,curr.timestamp.max(newdata.timestamp),curr.temperature.min(newdata.temperature)))
    //分流
    val splitStream=dataStream.split( data => {
      if(data.temperature >30){
        Seq("hight")
      }else(
        Seq("low")
      )
    })

    val hightStream=splitStream.select("hight")
    val lowStream=splitStream.select("low")
    //合流
    val warningStream: DataStream[(String, Double)] = hightStream.map(
      data => (data.id, data.temperature)
      //      new MyMapper
    )
    val connectedStreams=hightStream.connect(lowStream)
    hightStream.print()
    //lowStream.print()
    //arrStream.print()
    env.execute()
  }
}
