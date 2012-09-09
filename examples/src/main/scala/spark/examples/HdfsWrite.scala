package spark.examples

import spark.SparkContext
import spark.SparkContext._

import java.io.{OutputStream, ObjectOutputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsWrite {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWrite <host> <pathToOutputDir> [numMappers = 2] [MBPerMapper = 1] [numReplicas = 2]")
      System.exit(1)
    }

    val pathToOutputDir = args(1)
    val numMappers = if (args.length > 2) args(2).toInt else 2
    val mbPerMapper = if (args.length > 3) args(3).toDouble else 1.0
    val bytesToWrite = (mbPerMapper * 1024.0 * 1024.0).toLong
    val numRep = if (args.length > 4) args(4).toInt else 2

    val sc = new SparkContext(args(0), "HdfsWrite")

    sc.parallelize(0 until numMappers, numMappers).foreach { id =>
      val out = openFileForWriting(getDfsAddress(pathToOutputDir), pathToOutputDir + "/" + id, numRep)

      val BUFFER_SIZE = 4 * 1024 * 1024
      var b = new Array[Byte](BUFFER_SIZE)

      var bytesWritten = 0L
      while (bytesWritten < bytesToWrite) {
        val btw = Math.min(bytesToWrite - bytesWritten, BUFFER_SIZE).toInt
        if (btw < BUFFER_SIZE) {
          b = new Array[Byte](btw)
        }
        out.write(b, 0, btw)
        bytesWritten += btw
      }
      out.close
    }

    System.exit(0)
  }

  def getDfsAddress(pathToDir: String) = {
    val lc = pathToDir.lastIndexOf(":")
    val ns = pathToDir.substring(lc).indexOf("/")
    pathToDir.substring(0, lc + ns)
  }

  def openFileForWriting(dfsAddress: String, pathToFile: String, numRep: Int): OutputStream = {
    val conf = new Configuration()
    conf.setInt("dfs.replication", numRep)
    val fileSystem = FileSystem.get(new URI(dfsAddress), conf)
    fileSystem.create(new Path(pathToFile))
  }
}
