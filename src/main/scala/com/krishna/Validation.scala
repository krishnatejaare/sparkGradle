package com.krishna

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.functions.{col, lit, when}

object Validation {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var ignore=1;
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/IDS 1.0.txt")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/IDS reports.txt")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/krishnateja/Desktop/IDS Critical reports.txt")

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
    var finalcritical_absent=criticalkpiabsentin2dot0.withColumn("2dotFact", lit("Not Present in 2.0")).withColumn("deviation", lit("not determined"))
      .withColumn("status",lit("Critical"))
      .withColumn("difference",lit("not determined"))

    finalcritical_absent.repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/critical_absent")
    var criticalkpiabsentin2dot0_count = criticalkpiabsentin2dot0.withColumn("2dotFact", lit("Not Present in 2.0")).withColumn("deviation", lit("not determined")).count
    println("The total count of Critical KPI's not present 2.0 : "+criticalkpiabsentin2dot0_count)
    println()
    println("___________________________________________________________________________________")
    println("These are the Critical KPI's present in 2.0")
    println("the final critical deviation is")
    finalcriticaldeviation.repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/critical_present")
    println("___________________________________________________________________________________")
    var total_kpis_in_1dot0_wrt_reports = criticalkpicount
    var total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical = finalcriticaldeviation.count
    println("The total Critical KPI's : "+criticalkpicount)
    println("Critical KPI's present in 2.0 : "+ total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical)
    println("Critical KPI's not present 2.0 : "+criticalkpiabsentin2dot0_count)
    println("___________________________________________________________________________________")
    println()
    var nonpositive_critical_deviation_percent = (((finalcriticaldeviation.filter("Deviation<='0'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var postive_critical_deviation_percent = (((finalcriticaldeviation.filter("Deviation>'0'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100

    finalcriticaldeviation.filter("Deviation='0'").count
    var postive_critical_deviation_count = finalcriticaldeviation.filter("Deviation>'0'").count
    if (postive_critical_deviation_count == 0) {
      postive_critical_deviation_count = 1
    }

    var critical_deviation_001_percentage = (((finalcriticaldeviation.filter("Deviation>'0' and Deviation<='0.01'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var critical_deviation_005_percentage = (((finalcriticaldeviation.filter("Deviation>'0.01' and Deviation<='0.05'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var critical_deviation_01_percentage = (((finalcriticaldeviation.filter("Deviation>'0.05' and Deviation<='0.1'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var critical_deviation_1_percentage = (((finalcriticaldeviation.filter("Deviation>'0.1' and Deviation<='1'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var critical_deviation_5_percentage = (((finalcriticaldeviation.filter("Deviation>'1' and Deviation<='5'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100
    var critical_deviation_greaterthan5_percentage = (((finalcriticaldeviation.filter("Deviation>'5'").count)).toDouble / total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical) * 100

    var optionalkpi_1dot0 = df1.except(df1withreport)
    var total_optional_kpis_in_1dot0_wrt_reports = optionalkpi_1dot0.count
    optionalkpi_1dot0.registerTempTable("optionalkpifor1dot0")

    var optionalkpiof1notpresentin2_notdistinct = sqlContext.sql("select optionalkpifor1dot0.date,optionalkpifor1dot0.kpi,optionalkpifor1dot0.1dotFact from optionalkpifor1dot0 where optionalkpifor1dot0.kpi not in (select kpi from secondtable)")
    var optionalkpiof1notpresentin2=optionalkpiof1notpresentin2_notdistinct.distinct()
    var optionalkpiof1presentin2_notdistinct = sqlContext.sql("select secondtable.date,secondtable.kpi,optionalkpifor1dot0.1dotFact,secondtable.2dotFact from optionalkpifor1dot0,secondtable where optionalkpifor1dot0.kpi=secondtable.kpi")
    var optionalkpiof1presentin2=optionalkpiof1presentin2_notdistinct.distinct()
    var total_optional_kpis_in_2dot0_which_are_common_in_1dot0 = optionalkpiof1presentin2.count
    println("___________________________________________________________________________________")
    println("___________________________________________________________________________________")
    println("The Optional KPI's that are not present in 2.0 but present in 1.0")

    var finaloptional_absent=optionalkpiof1notpresentin2.withColumn("2dotFact", lit("Not Present in 2.0")).withColumn("deviation", lit("not determined"))
      .withColumn("status",lit("Optional"))
      .withColumn("difference",lit("not determined"))

    finaloptional_absent.repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/optional_absent")
    var optionalkpiof1notpresentin2_count = optionalkpiof1notpresentin2.count
    //    println("The total count of Optional KPI's that are not present in 2.0 "+optionalkpiof1notpresentin2_count)
    println("___________________________________________________________________________________")
    println("The Optional KPI's that are common in 1.0 and 2.0")

    var final_optionalkpiof1presentin2_deviation = optionalkpiof1presentin2.withColumn("deviation", ((optionalkpiof1presentin2("1dotFact") - optionalkpiof1presentin2("2dotFact")) * 100) / optionalkpiof1presentin2("1dotFact"))
      .withColumn("status",lit("Optional"))
      .withColumn("difference",(optionalkpiof1presentin2("1dotFact") - optionalkpiof1presentin2("2dotFact")))
    final_optionalkpiof1presentin2_deviation.repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/optional_present")

    var nonpositive_optional_deviation_percent = (((final_optionalkpiof1presentin2_deviation.filter("Deviation<='0'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    var postive_optional_deviation_percent = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'0'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100


    var postive_optional_deviation_count = final_optionalkpiof1presentin2_deviation.filter("Deviation>'0'").count
    if (postive_optional_deviation_count == 0) {
      postive_optional_deviation_count = 1
    }

//    println("The yellow optional deviation is below 0.1% ")

    var optional_deviation_001_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'0' and Deviation<='0.01'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    var optional_deviation_005_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'0.01' and Deviation<='0.05'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    var optional_deviation_01_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'0.05' and Deviation<='0.1'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100

//    println("The Red optional deviation is above 0.1%")
    var optional_deviation_1_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'0.1' and Deviation<='1'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    var optional_deviation_5_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'1' and Deviation<='5'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    var optional_deviation_greaterthan5_percentage = (((final_optionalkpiof1presentin2_deviation.filter("Deviation>'5'").count)).toDouble / total_optional_kpis_in_2dot0_which_are_common_in_1dot0) * 100
    println("___________________________________________________________________________________")
    println("The total Optional KPI's : "+total_optional_kpis_in_1dot0_wrt_reports)
    println("Optional KPI's present in 2.0 : "+ total_optional_kpis_in_2dot0_which_are_common_in_1dot0)
    println("Optional KPI's not present 2.0 : "+optionalkpiof1notpresentin2_count)
    println("___________________________________________________________________________________")
    println("___________________________________________________________________________________")
    println("__________________________________Critical KPI Absent in 2.0______________________________________________________")
    println("The total count of Critical KPI's not present 2.0 : " + criticalkpiabsentin2dot0_count)
    //criticalkpiabsentin2dot0.foreach(x => println(x(1)))
    println("___________________________________________________________________________________")

    println("__________________________________For Critical KPI Present in 2.0 ________________________________________________")
    println("Total number of KPIs in 1.0 wrt reports :" + total_kpis_in_1dot0_wrt_reports)
    println("2.0 KPIs that exist in 1.0 (Self Serve Generated) :" + total_kpis_in_2dot0_which_are_common_in_1dot0_and_critical)
    println("2.0 facts >= 1.0 facts  :" + nonpositive_critical_deviation_percent + "%")
    println("2.0 facts < 1.0 facts  :" + postive_critical_deviation_percent + "%")
    println("The yellow deviation ")
    println("deviation <= 0.01% :" + critical_deviation_001_percentage + "%")
    println("deviation > 0.01% & <=0.05% :" + critical_deviation_005_percentage + "%")
    println("deviation > 0.05% & <=0.1% :" + critical_deviation_01_percentage + "%")
    println("The Red deviation ")
    println("deviation > 0.1% & <=1% :" + critical_deviation_1_percentage + "%")
    println("deviation > 1% & <=5% :" + critical_deviation_5_percentage + "%")
    println("critical_deviation_greaterthan5_percentage :" + critical_deviation_greaterthan5_percentage + "%")
    println("___________________________________________________________________________________")

    println("___________________________________List of KPI's in Red Level Deviation________________________________________________")
    println("deviation > 0.1% & <=1% :")
    finalcriticaldeviation.filter("Deviation>'0.1' and Deviation<='1' and difference >"+ignore).show(100000, false)
    finalcriticaldeviation.filter("Deviation>'0.1' and Deviation<='1' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/critical_dev_lessthan_1")

    println("deviation > 1% & <=5% :")
    finalcriticaldeviation.filter("Deviation>'1' and Deviation<='5' and difference >"+ignore).show(100000, false)
    finalcriticaldeviation.filter("Deviation>'1' and Deviation<='5' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/critical_dev_between_1to5")

    println("deviation > 5% :")
    finalcriticaldeviation.filter("Deviation>'5' and difference >"+ignore).show(100000, false)
    finalcriticaldeviation.filter("Deviation>'5' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/critical_dev_greaterthan_5")

    println("___________________________________________________________________________________")

    println("__________________________________Optional KPI Absent in 2.0________________________________________________")
    println("The total count of Optional KPI's that are not present in 2.0 : " + optionalkpiof1notpresentin2_count)
    //optionalkpiof1notpresentin2.foreach(x => println(x(1)))
    println("___________________________________________________________________________________")

    println("__________________________________For Optional KPI Present in 2.0________________________________________________")
    println("Total number of KPIs in 1.0 but not in reports :" + total_optional_kpis_in_1dot0_wrt_reports)
    println("2.0 KPIs that exist in 1.0 (Self Serve Generated) :" + total_optional_kpis_in_2dot0_which_are_common_in_1dot0)
    println("2.0 facts >= 1.0 facts  :" + nonpositive_optional_deviation_percent + "%")
    println("2.0 facts < 1.0 facts  :" + postive_optional_deviation_percent + "%")
    println("The yellow deviation ")
    println("deviation <= 0.01% :" + optional_deviation_001_percentage + "%")
    println("deviation > 0.01% & <=0.05% :" + optional_deviation_005_percentage + "%")
    println("deviation > 0.05% & <=0.1% :" + optional_deviation_01_percentage + "%")
    println("The Red deviation ")
    println("deviation > 0.1% & <=1% :" + optional_deviation_1_percentage + "%")
    println("deviation > 1% & <=5% :" + optional_deviation_5_percentage + "%")
    println("deviation > 5% :" + optional_deviation_greaterthan5_percentage + "%")
    println("___________________________________________________________________________________")


    println("___________________________________List of Optional KPI's in Red Level Deviation________________________________________________")
    println("List of KPI's who have Red Level Deviation")
    println("deviation > 0.1% & <=1% :")
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'0.1' and Deviation<='1' and difference >"+ignore).show(100000, false)
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'0.1' and Deviation<='1' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/optional_dev_lessthan_1")

    println("deviation > 1% & <=5% :")
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'1' and Deviation<='5' and difference >"+ignore).show(100000, false)
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'1' and Deviation<='5' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/optional_dev_between_1to5")

    println("deviation > 5% :")
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'5' and difference >"+ignore).show(100000, false)
    final_optionalkpiof1presentin2_deviation.filter("Deviation>'5' and difference >"+ignore).repartition(1).write.format("csv").mode("overwrite").save("/Users/krishnateja/Downloads/scala-spark-tutorial-master/src/main/scala/com/krishna/optional_dev_greaterthan5")

    println("___________________________________________________________________________________")

    //only_in_reports.show(1000,false)
//    finalcritical_absent.union(finaloptional_absent).union(finalcriticaldeviation).union(final_optionalkpiof1presentin2_deviation).show(10000000,false)
  }
}
