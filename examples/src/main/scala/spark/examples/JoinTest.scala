package spark.examples

import spark.SparkContext
import spark.SparkContext._
import java.util.Random

object JoinTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "Join Test")
    
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined1 = rdd1.join(rdd2)

    val rdd3 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd4 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined2 = rdd3.join(rdd4)

    println(joined1.join(joined2).collect())

    System.exit(0)
  }
}
