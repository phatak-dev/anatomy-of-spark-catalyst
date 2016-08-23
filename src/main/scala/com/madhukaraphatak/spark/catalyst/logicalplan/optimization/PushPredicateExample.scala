package com.madhukaraphatak.spark.catalyst.logicalplan.optimization

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughProject
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}
import org.apache.spark.sql.types.IntegerType

/**
  * Created by madhu on 24/07/16.
  */
object PushPredicateExample {
  def main(args: Array[String]): Unit = {

   val attributeReference = AttributeReference("id",IntegerType)()
   val attributes = Seq(attributeReference)
   val localRelation = LocalRelation(attributes)

    val projectPlan = Project(Seq(attributeReference),localRelation)
    val booleanExpression = EqualTo(attributeReference,Literal(10))
    val filterLogicalPlan = Filter(booleanExpression,projectPlan)

    val analyzedPlan = SimpleAnalyzer.execute(filterLogicalPlan)
    println(s"analysed plan is \n ${filterLogicalPlan.numberedTreeString}")

    //filter push example

    val filterPushOptimizedPlan = PushPredicateThroughProject(analyzedPlan)

    println(s"analysed plan is \n ${filterPushOptimizedPlan.numberedTreeString}")

  }

}
