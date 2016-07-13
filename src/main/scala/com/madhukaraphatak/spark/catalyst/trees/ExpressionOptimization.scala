package com.madhukaraphatak.spark.catalyst.trees
/**
  * Created by madhu on 13/07/16.
  */
object ExpressionOptimization {

  def main(args: Array[String]) {

    lazy val mult1LeafValue =  LeafValue(19)
    lazy val mult2LeafValue = LeafValue(0)

    //single level of nesting
    val multiplicationOptimization = MUL(mult2LeafValue,ADD(mult1LeafValue,mult2LeafValue))

    val optimizedMultiplication = multiplicationOptimization transform {
      case MUL(_,LeafValue(0)) => LeafValue(0)
      case MUL(LeafValue(0),_) => LeafValue(0)
    }

    println(optimizedMultiplication.evaluate)

    //multi level optimization
    val mul = MUL(mult1LeafValue,MUL(LeafValue(10), LeafValue(0))) transformUp {
      case MUL(_,LeafValue(0)) => LeafValue(0)
      case MUL(LeafValue(0),_) => LeafValue(0)
    }

    println(mul.evaluate)

  }

}
