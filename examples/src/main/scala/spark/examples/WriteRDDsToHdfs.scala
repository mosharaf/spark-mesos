package spark.examples

import spark.SparkContext
import spark.SparkContext._

object WriteRDDsToHdfs {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WriteRDDsToHdfs <host> <pathToOutputFile> [numMappers] [MBPerMapper]")
      System.exit(1)
    }  
    
    val pathToOutputFile = args(1)
    val numMappers = if (args.length > 2) args(2).toInt else 2
    val mbPerMapper = if (args.length > 3) args(3).toInt else 1
    var bytesPerMapper = mbPerMapper * (1024 * 1024)

    val sc = new SparkContext(args(0), "WriteRDDsToHdfs", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_EXAMPLES_JAR")))
    
    val rdd1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val byteArr = new StringBuilder(bytesPerMapper)
      (0 until bytesPerMapper).foreach { _ => byteArr.append(' ')}
      byteArr
    }.cache
    // Enforce that everything has been calculated and in cache
    println(rdd1.count)    

    rdd1.saveAsTextFile(pathToOutputFile)

    System.exit(0)
  }
}
