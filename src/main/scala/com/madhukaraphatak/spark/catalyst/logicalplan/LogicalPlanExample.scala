package com.madhukaraphatak.spark.catalyst.logicalplan

import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}
import org.apache.spark.sql.types.IntegerType


/**
  * Created by madhu on 23/07/16.
  */
object LogicalPlanExample {

  def main(args: Array[String]): Unit = {

    //simple leaf logical plan

    val attributeReference = AttributeReference("id",IntegerType)()
    val attributes = Seq(attributeReference)
    val localRelation= LocalRelation(attributes)
    println("local relation tree")
    println(localRelation.numberedTreeString)
    
    //filter plan
    val equalToExpression= EqualTo(attributeReference,Literal(10))
    val filterLogicalPlan = Filter(equalToExpression,localRelation)
    println("filter plan tree is")
    println(filterLogicalPlan.numberedTreeString)

    //Project Plan
    val projectPlan = Project(Seq(attributeReference),localRelation)
    println("project plan tree is")
    println(projectPlan.numberedTreeString)

    //project over aggregation expression
    val aggregateExpression = Alias(Sum(attributeReference),"idSum")()
    val projectSum = Project(Seq(aggregateExpression),localRelation)
    println(" project sum plan tree is")
    println(projectSum.numberedTreeString)
    
    //project over count expression
    val aggregateCount = Alias(Count(attributeReference),"idCount")()
    val projectCount = Project(Seq(aggregateCount ),localRelation)
    println(" project count plan tree is")
    println(aggregateCount.numberedTreeString)

  }

}
