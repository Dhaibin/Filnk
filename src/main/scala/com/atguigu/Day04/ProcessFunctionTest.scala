package com.atguigu.Day04

import com.atguigu.Day02.SensorReading
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("hadoop105", 7777)
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
      } )

    val warningStream=dataStream.keyBy("Id")
      .process( new TempIncreWarning(1000L))

    warningStream.print()
    env.execute("process function job")
  }
}
class TempIncreWarning(interval:Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{
  lazy val lastTempState=getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  lazy val curTimerTsState=getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //取出状态
    val lastTemp=lastTempState.value()
    val curTimerTs=curTimerTsState.value()
    lastTempState.update(value.temperature)
    if(value.temperature>lastTemp &&curTimerTs ==0){
      val ts=ctx.timerService().currentProcessingTime() +interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerTsState.update(ts)
    }else if(value.temperature < lastTemp){
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      curTimerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect( "温度值连续" + interval/1000 + "秒上升" )
    curTimerTsState.clear()

  }
}