package com.madhukaraphatak.spark.catalyst

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
  * Created by madhu on 13/07/16.
  */
package object trees {
  trait ArithmeticExpression extends TreeNode[ArithmeticExpression] {
    type EvaluationType
    def evaluate:EvaluationType
  }
  case class LeafValue(value:Double) extends ArithmeticExpression {
    type EvaluationType = Any
    override def evaluate: Any = value

    override def children: Seq[ArithmeticExpression] = Nil
  }

  case class ADD(left:ArithmeticExpression, right: ArithmeticExpression) extends
    ArithmeticExpression {
    type EvaluationType = Any

    override def evaluate = {
      left.evaluate.toString.toDouble + right.evaluate.toString.toDouble
    }

    override def children: Seq[ArithmeticExpression] = Seq(left,right)
  }

  case class Divide(left:ArithmeticExpression, right: ArithmeticExpression) extends
    ArithmeticExpression {
    override type EvaluationType = Any

    override def evaluate = left.evaluate.toString.toDouble / right.evaluate.toString.toDouble

    override def children: Seq[ArithmeticExpression] = Seq(left,right)
  }

  case class MUL(left:ArithmeticExpression, right: ArithmeticExpression) extends
    ArithmeticExpression  {
    override type EvaluationType = Any
    override def evaluate = left.evaluate.toString.toDouble * right.evaluate.toString.toDouble

    override def children: Seq[ArithmeticExpression] = Seq(left,right)
  }


}
