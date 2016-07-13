package com.madhukaraphatak.spark.catalyst.trees

/**
  * Example to show how to use expression tree
  */
object ExpressionExample {

  def main(args: Array[String]) {

    val leafNode1 = new LeafValue(10)
    val leafNode2 = new LeafValue(20)

    val addNode = ADD(leafNode1,leafNode2)

    val complexAddNode = ADD(addNode,LeafValue(50))

    println(addNode.evaluate)

    println(complexAddNode.evaluate)

    val divideNode = Divide(addNode,LeafValue(0))

    println(divideNode.evaluate)

  }
}
