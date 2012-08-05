package spark.executor

import java.nio.ByteBuffer
import spark.Logging
import spark.TaskState.TaskState
import spark.util.AkkaUtils
import akka.actor.{ActorRef, Actor, Props}
import java.util.concurrent.{TimeUnit, ThreadPoolExecutor, SynchronousQueue}
import akka.remote.RemoteClientLifeCycleEvent
import spark.scheduler.cluster._
import spark.scheduler.cluster.RegisteredSlave
import spark.scheduler.cluster.LaunchTask
import spark.scheduler.cluster.RegisterSlaveFailed
import spark.scheduler.cluster.RegisterSlave

import java.io.{InputStreamReader, BufferedReader}

class StandaloneExecutorBackend(
    executor: Executor,
    masterUrl: String,
    slaveId: String,
    hostname: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  val BANDWIDTH_INTERVAL = System.getProperty("spark.bandwidth.interval", "100").toLong
    
  val commandToGetRxBytes = System.getProperty("spark.command.getRxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $7}'")
  // val commandToGetRxBytes = System.getProperty("spark.command.getRxBytes", "ifconfig eth0 | grep \"RX bytes\" | cut -d: -f2 | awk '{ print $1 }'")
  val commandToGetTxBytes = System.getProperty("spark.command.getTxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $10}'")
  // val commandToGetTxBytes = System.getProperty("spark.command.getTxBytes", "ifconfig eth0 | grep \"TX bytes\" | cut -d: -f3 | awk '{ print $1 }'")

  var lastRxBps = -1.0
  var lastRxBytes = getValueFromCommandLine(commandToGetRxBytes).toDouble
  var lastTimeRxCalcMillis = System.currentTimeMillis

  var lastTxBps = -1.0
  var lastTxBytes = getValueFromCommandLine(commandToGetTxBytes).toDouble
  var lastTimeTxCalcMillis = lastTimeRxCalcMillis

  val threadPool = new ThreadPoolExecutor(
    1, 128, 600, TimeUnit.SECONDS, new SynchronousQueue[Runnable])

  var master: ActorRef = null

  override def preStart() {
    try {
      logInfo("Connecting to master: " + masterUrl)
      master = context.actorFor(masterUrl)
      master ! RegisterSlave(slaveId, hostname, cores, getRxBps(), getTxBps())
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master) // Doesn't work with remote actors, but useful for testing
    } catch {
      case e: Exception =>
        logError("Failed to connect to master", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisteredSlave(sparkProperties) =>
      logInfo("Successfully registered with master")
      executor.initialize(hostname, sparkProperties)

    case RegisterSlaveFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    master ! StatusUpdate(slaveId, taskId, state, data, getRxBps(), getTxBps())
  }
  
  def getRxBps(): Double = synchronized {
    val curTime = System.currentTimeMillis
    val millisElapsed = curTime - lastTimeRxCalcMillis
    if (millisElapsed < BANDWIDTH_INTERVAL && lastRxBps >= 0.0) {
      return lastRxBps
    }
    val secondsElapsed = millisElapsed / 1000.0
    lastTimeRxCalcMillis = curTime
    val curRxBytes = getValueFromCommandLine(commandToGetRxBytes).toDouble
    val rxBps = (curRxBytes - lastRxBytes) / secondsElapsed
    lastRxBytes = curRxBytes
    lastRxBps = rxBps
    rxBps
  }

  def getTxBps(): Double = synchronized {
    val curTime = System.currentTimeMillis
    val millisElapsed = curTime - lastTimeTxCalcMillis
    if (millisElapsed < BANDWIDTH_INTERVAL && lastTxBps >= 0.0) {
      return lastTxBps
    }
    val secondsElapsed = millisElapsed / 1000.0
    lastTimeTxCalcMillis = curTime
    val curTxBytes = getValueFromCommandLine(commandToGetTxBytes).toDouble
    val txBps = (curTxBytes - lastTxBytes) / secondsElapsed
    lastTxBytes = curTxBytes
    lastTxBps = txBps
    txBps
  }

  def getValueFromCommandLine(commandToRun: String): String = {
    var retVal:String = null
    try {
      val pb = new java.lang.ProcessBuilder("/bin/sh", "-c", commandToRun)
      val p = pb.start()
      val stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()))
      retVal = stdInput.readLine()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    retVal
  }
}

object StandaloneExecutorBackend {
  def run(masterUrl: String, slaveId: String, hostname: String, cores: Int) {
    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(new Executor, masterUrl, slaveId, hostname, cores)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: StandaloneExecutorBackend <master> <slaveId> <hostname> <cores>")
      System.exit(1)
    }
    run(args(0), args(1), args(2), args(3).toInt)
  }
}
