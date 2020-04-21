package com.atguigu.Day01

import scala.util.Random

object test {
  def main(args: Array[String]): Unit = {
    val rand =new Random()

    val range = new Range(10,20,2)
    range.toList.foreach(print)
  }
}
