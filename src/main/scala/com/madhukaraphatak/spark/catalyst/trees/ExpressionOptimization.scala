package com.madhukaraphatak.spark.catalyst.trees
/**
  * Created by madhu on 13/07/16.
  */
object ExpressionOptimization {

  def main(args: Array[String]) {

    lazy val mult1LeafValue =  LeafValue(19)
    lazy val mult2LeafValue = LeafValue(0)

    //single level of nesting
    val multiplicationOptimization = MUL(mult1LeafValue,MUL(mult1LeafValue,mult2LeafValue))

    val transformOptimization = multiplicationOptimization transform {
      case MUL(_,LeafValue(0)) => LeafValue(0)
      case MUL(LeafValue(0),_) => LeafValue(0)
    }
    println("transform optimization")
    println(transformOptimization.numberedTreeString)

    //multi level optimization
    val transformUpOptimization = MUL(mult1LeafValue,MUL(LeafValue(10), LeafValue(0))) transformUp {
      case MUL(_,LeafValue(0)) => LeafValue(0)
      case MUL(LeafValue(0),_) => LeafValue(0)
    }

    println("transformUp optimization")
    println(transformUpOptimization.numberedTreeString)

  }

}
