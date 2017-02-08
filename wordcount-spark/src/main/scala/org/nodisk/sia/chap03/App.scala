package org.nodisk.sia.chap03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.io.Source.fromFile

object App {
  val INPUT_FILE_LOCATION = "file:///E:/tmp/"

  def main(args:Array[String])={
    
    // Initialize Spark configuration
    val conf = new SparkConf()
                      .setAppName("Github push counter")
                      
    // Create Spark context
    val sc = new SparkContext(conf)
    
    // Create Spark SQL context
    val sqlContext = new SQLContext(sc)
    
    // Load json file
    val ghLog = sqlContext.read.json(INPUT_FILE_LOCATION+args(0))
    
    //  print inferred schema
    ghLog.printSchema

    // Filter json records for field "type" with values 'PushEvent'
    val pushes = ghLog.filter("type = 'PushEvent'")
    
    // print sample data
    
    println("all events : "+ghLog.count)
    println("only pushes : "+pushes.count)
    
    pushes.show(5)
    
    // Group by actor.login and get count
    val groupByLogin = pushes.groupBy("actor.login").count
    
    groupByLogin.show(5)
    
    // Order in desc the max pushes
    val orderByPushes = groupByLogin.orderBy(groupByLogin("count").desc)
    
    orderByPushes.show(5)
    
    // Excluding user contained in ghEmployees.txt
    val excludeFilePath = INPUT_FILE_LOCATION+args(1)
    
    // Read all lines from the file
    var lines = for{
      line <- fromFile(excludeFilePath).getLines
    } yield line.trim
    
    val excludedUsers = Set() ++ lines
    
    // Stop Spark context
    sc.stop
  
  }
}