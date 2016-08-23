package com.madhukaraphatak.spark.catalyst.logicalplan.analysis

import org.apache.spark.sql.catalyst.{SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubQueries, SimpleAnalyzer, SimpleCatalog, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
  * Created by madhu on 23/07/16.
  */
object AnalysisExample {

  def main(args: Array[String]): Unit = {

    val attributeReference = AttributeReference("id",IntegerType)()
    val attributes = Seq(attributeReference)

    val booleanExpression = EqualTo(attributeReference,Literal(10))
    val localRelation = LocalRelation(attributes)
    val filterLogicalPlan = Filter(booleanExpression,localRelation)

    println("non analysed filter plan is")
    println(filterLogicalPlan.numberedTreeString)
    println(filterLogicalPlan.analyzed)


   SimpleAnalyzer.checkAnalysis(filterLogicalPlan)
   println("analysed filter plan is")
   println(filterLogicalPlan.numberedTreeString)
   println(filterLogicalPlan.analyzed)

   //analyse a wrong plan .. Puts an extra ' to signify not able to 
   // analyze properly 

   val wrongBooleanExpression = EqualTo(attributeReference,Literal(true))
   val wrongFilterPlan = Filter(wrongBooleanExpression, localRelation)
   println(wrongFilterPlan.numberedTreeString)
  }

}
