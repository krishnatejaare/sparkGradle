package com.krishna

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.functions.{col, lit, when}

object filewriter {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var ignore=1000;
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/winterpeg 1.0 daily df.txt")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/winterpeg 2.0 daily df.txt")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/winterpeg report df daily.txt")

    df1.registerTempTable("firsttable")
    df2.registerTempTable("secondtable")
    df3.registerTempTable("reportskpitable")

    var only_in_reports=sqlContext.sql("select reportskpitable.kpi from reportskpitable,firsttable where reportskpitable.kpi=firsttable.kpi")
    var df1withreport_notdistinct = sqlContext.sql("select firsttable.date,firsttable.kpi,firsttable.1dotFact from firsttable,reportskpitable where firsttable.kpi=reportskpitable.kpi")
    var df1withreport=df1withreport_notdistinct.distinct()
    var criticalkpicount = df1withreport.count
    df1withreport.registerTempTable("reportkpi")

    var criticaldeviation_notdistinct = sqlContext.sql("select reportkpi.date,reportkpi.kpi,reportkpi.1dotFact,secondtable.2dotFact from reportkpi,secondtable where reportkpi.kpi=secondtable.kpi")
    var criticaldeviation=criticaldeviation_notdistinct.distinct()
    var finalcriticaldeviation = criticaldeviation.withColumn("deviation", ((criticaldeviation("1dotFact") - criticaldeviation("2dotFact")) * 100) / criticaldeviation("1dotFact"))
      .withColumn("status",lit("Critical"))
      .withColumn("difference",(criticaldeviation("1dotFact") - criticaldeviation("2dotFact")))

    println("___________________________________________________________________________________")
    println("These are the Critical KPI's not present in 2.0")
    var criticalkpiabsentin2dot0_notdistinct = sqlContext.sql("select reportkpi.date,reportkpi.kpi,reportkpi.1dotFact from reportkpi where reportkpi.kpi not in (select kpi from secondtable)")
    var criticalkpiabsentin2dot0=criticalkpiabsentin2dot0_notdistinct.distinct()
    println(criticalkpiabsentin2dot0.show(100,false))

  }
}
