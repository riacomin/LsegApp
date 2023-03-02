package org.lseg.exercise

import org.apache.spark.sql.SparkSession

trait SparkTrait {
  System.setProperty("hadoop.home.dir", "C:\\Users\\radui\\IdeaProjects\\LsegApp\\src\\resources\\winutils") //TODO replace with location of winutils
  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LsegMatchingEngine")
    .getOrCreate()

}
