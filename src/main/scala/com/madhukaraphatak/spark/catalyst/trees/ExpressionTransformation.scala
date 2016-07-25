package com.madhukaraphatak.spark.catalyst.trees

/**
  * Created by madhu on 13/07/16.
  */
object ExpressionTransformation {

  def main(args: Array[String]) {
    val leafNode1 = new LeafValue(10)
    val leafNode2 = new LeafValue(20)

    val addNode = ADD(leafNode1,leafNode2)
    val complexAddNode = ADD(addNode,LeafValue(50))
    val divideNode = Divide(addNode,LeafValue(0))

    println(divideNode.evaluate)
    //tree operations
    val transformedExpression = divideNode transform   {
      case expression @Divide(_,LeafValue(0)) => LeafValue(Double.NaN)
    }
    println(transformedExpression.evaluate)
  }

}
