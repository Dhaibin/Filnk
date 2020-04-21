package com.atguigu.Day03.sinkTest

import com.atguigu.Day02.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object ReidsSink {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputstream=env.readTextFile("D:\\scalacode\\Filnk\\src\\main\\resources\\sensor.txt")
    val dataStream=inputstream.map(
      data =>{
        val dataArray=data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })
    val conf=new FlinkJedisPoolConfig.Builder()
        .setHost("hadoop105")
        .setPort(6379)
      .build()
    val MyMapper=new RedisMapper[SensorReading] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
      }
      override def getKeyFromData(t: SensorReading): String = t.id

      override def getValueFromData(t: SensorReading): String = t.temperature.toString
    }
    dataStream.addSink(new RedisSink[SensorReading](conf,MyMapper))
    env.execute("redis-sink test")
  }
}
