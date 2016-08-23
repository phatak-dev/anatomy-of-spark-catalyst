package com.madhukaraphatak.spark.catalyst.logicalplan

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.IntegerType

/**
  * Created by madhu on 23/07/16.
  */
object FilterLogicalPlanManipulation {

  class BooleanFoldingRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressionsUp{
      case equal @ EqualTo(left,right) => if(left == Literal(true) || right==Literal(true))  Literal(true)
      else equal
    }
  }

  class BooleanRemove extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Filter(expression, child) => if (expression == Literal(true)) child else plan
      case _ => plan
    }
  }


  def main(args: Array[String]): Unit = {
    //create local relation with single column
    val attributes = Seq(AttributeReference("id",IntegerType)())
    val localRelation = LocalRelation(attributes)

    // create filter where we are comparing to a true value
    val booleanExpression = EqualTo(BoundReference(0,IntegerType,false),Literal(true))
    val filterLogicalPlan = Filter(booleanExpression,localRelation)

    println("original filter plan")
    println(filterLogicalPlan.numberedTreeString)

    //rule which removes the comparison and substitues the true literal
    val booleanFoldingRule = new BooleanFoldingRule()
    val optimizedRule = booleanFoldingRule.apply(filterLogicalPlan)
    println(" filter plan after applying boolean folding rule")
    println(optimizedRule.numberedTreeString)
    
    //apply boolean removal
    val booleanRemove = new BooleanRemove()
    val afterBooleanRemoval = booleanRemove(optimizedRule)
    println(" filter plan after applying boolean removal rule")
    println(afterBooleanRemoval)

  }

}
