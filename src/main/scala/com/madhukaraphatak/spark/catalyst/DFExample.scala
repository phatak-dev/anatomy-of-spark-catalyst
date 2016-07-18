package com.madhukaraphatak.spark.catalyst

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by madhu on 9/7/16.
  */
object DFExample {

  def main(args: Array[String]) {

   val sqlContext = new SQLContext(new SparkContext("local","test"))

    val rdd = sqlContext.sparkContext.makeRDD(List(("hi","1")))

    val dataFrame = sqlContext.createDataFrame(rdd)

    val selectedDataFrame = dataFrame.select("_1")

    selectedDataFrame.explain(true)
  }
}
