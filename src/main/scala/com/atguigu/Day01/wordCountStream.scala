package com.atguigu.Day01
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random


object wordCountStream {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
//    val params= ParameterTool.fromArgs(args)
//    val host=params.get("host")
//    val port=params.getInt("port")

    val wordcountStream:DataStream[String] = env.socketTextStream("hadoop105",9999)
    val dataStream: DataStream[(String, Int)] = wordcountStream.flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
    dataStream.print()
    env.execute("socket stream word count")
  }
}
