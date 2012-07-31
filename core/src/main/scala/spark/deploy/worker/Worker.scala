package spark.deploy.worker

import scala.collection.mutable.{ArrayBuffer, HashMap}
import akka.actor.{ActorRef, Props, Actor}
import spark.{Logging, Utils}
import spark.util.AkkaUtils
import spark.deploy._
import akka.remote.RemoteClientLifeCycleEvent
import java.text.SimpleDateFormat
import java.util.Date
import akka.remote.RemoteClientShutdown
import akka.remote.RemoteClientDisconnected
import spark.deploy.RegisterWorker
import spark.deploy.LaunchExecutor
import spark.deploy.RegisterWorkerFailed
import spark.deploy.UpdateNetworkLoad
import akka.actor.Terminated
import java.io.{File, InputStreamReader, BufferedReader}

class Worker(ip: String, port: Int, webUiPort: Int, cores: Int, memory: Int, masterUrl: String)
  extends Actor with Logging {

  val commandToGetRxBytes = System.getProperty("spark.command.getRxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $7}'")
  val commandToGetTxBytes = System.getProperty("spark.command.getTxBytes", "netstat -ib | grep mosharaf-mb | awk '{print $10}'")

  var lastRxBytes = getValueFromCommandLine(commandToGetRxBytes).toDouble
  var lastTxBytes = getValueFromCommandLine(commandToGetTxBytes).toDouble
  var lastTimeRxCalcMillis = System.currentTimeMillis
  var lastTimeTxCalcMillis = lastTimeRxCalcMillis

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For worker and executor IDs
  val MASTER_REGEX = "spark://([^:]+):([0-9]+)".r

  var master: ActorRef = null
  val workerId = generateWorkerId()
  var sparkHome: File = null
  var workDir: File = null
  val executors = new HashMap[String, ExecutorRunner]
  val finishedExecutors = new ArrayBuffer[String]

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def getRxBps(): Double = {
    val curTime = System.currentTimeMillis
    val secondsElapsed = (curTime - lastTimeRxCalcMillis) / 1000.0
    lastTimeRxCalcMillis = curTime
    val curRxBytes = getValueFromCommandLine(commandToGetRxBytes).toDouble
    val rxBps = (curRxBytes - lastRxBytes) / secondsElapsed
    lastRxBytes = curRxBytes
    rxBps
  }

  def getTxBps(): Double = {
    val curTime = System.currentTimeMillis
    val secondsElapsed = (curTime - lastTimeTxCalcMillis) / 1000.0
    lastTimeTxCalcMillis = curTime
    val curTxBytes = getValueFromCommandLine(commandToGetTxBytes).toDouble
    val txBps = (curTxBytes - lastTxBytes) / secondsElapsed
    lastTxBytes = curTxBytes;
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

  def startNetworkUpdater() {
    val t = new Thread {
      override def run() {
        while (true) {
          Thread.sleep(1000)
          master ! UpdateNetworkLoad(workerId, getRxBps(), getTxBps())
        }
      }
    }
    t.setDaemon(true)
    t.start()
  }
  
  def createWorkDir() {
    workDir = new File(sparkHome, "work")
    try {
      if (!workDir.exists() && !workDir.mkdirs()) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def preStart() {
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      ip, port, cores, Utils.memoryMegabytesToString(memory)))
    val envVar = System.getenv("SPARK_HOME")
    sparkHome = new File(if (envVar == null) "." else envVar)
    logInfo("Spark home: " + sparkHome)
    createWorkDir()
    connectToMaster()
    startWebUi()
  }

  def connectToMaster() {
    masterUrl match {
      case MASTER_REGEX(masterHost, masterPort) => {
        logInfo("Connecting to master spark://" + masterHost + ":" + masterPort)
        val akkaUrl = "akka://spark@%s:%s/user/Master".format(masterHost, masterPort)
        try {
          master = context.actorFor(akkaUrl)
          master ! RegisterWorker(workerId, ip, port, cores, memory)
          context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
          context.watch(master) // Doesn't work with remote actors, but useful for testing
        } catch {
          case e: Exception =>
            logError("Failed to connect to master", e)
            System.exit(1)
        }
      }

      case _ =>
        logError("Invalid master URL: " + masterUrl)
        System.exit(1)
    }
  }

  def startWebUi() {
    val webUi = new WorkerWebUI(context.system, self)
    try {
      AkkaUtils.startSprayServer(context.system, ip, webUiPort, webUi.handler)
    } catch {
      case e: Exception =>
        logError("Failed to create web UI", e)
        System.exit(1)
    }
  }

  override def receive = {
    case RegisteredWorker =>
      logInfo("Successfully registered with master")

    case RegisterWorkerFailed(message) =>
      logError("Worker registration failed: " + message)
      System.exit(1)

    case LaunchExecutor(jobId, execId, jobDesc, cores_, memory_) =>
      logInfo("Asked to launch executor %s/%d for %s".format(jobId, execId, jobDesc.name))
      val manager = new ExecutorRunner(
        jobId, execId, jobDesc, cores_, memory_, self, workerId, ip, sparkHome, workDir)
      executors(jobId + "/" + execId) = manager
      manager.start()
      master ! ExecutorStateChanged(jobId, execId, ExecutorState.LOADING, None)

    case ExecutorStateChanged(jobId, execId, state, message) =>
      master ! ExecutorStateChanged(jobId, execId, state, message)
      if (ExecutorState.isFinished(state)) {
        logInfo("Executor " + jobId + "/" + execId + " finished with state " + state)
        executors -= jobId + "/" + execId
        finishedExecutors += jobId + "/" + execId
      }

    case KillExecutor(jobId, execId) =>
      val fullId = jobId + "/" + execId
      logInfo("Asked to kill executor " + fullId)
      executors(jobId + "/" + execId).kill()
      executors -= fullId
      finishedExecutors += fullId

    case Terminated(_) | RemoteClientDisconnected(_, _) | RemoteClientShutdown(_, _) =>
      masterDisconnected()
  }

  def masterDisconnected() {
    // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
    // (Note that if reconnecting we would also need to assign IDs differently.)
    logError("Connection to master failed! Shutting down.")
    executors.values.foreach(_.kill())
    System.exit(1)
  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(DATE_FORMAT.format(new Date), ip, port)
  }
}

object Worker {
  def main(argStrings: Array[String]) {
    val args = new WorkerArguments(argStrings)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", args.ip, args.port)
    val actor = actorSystem.actorOf(
      Props(new Worker(args.ip, boundPort, args.webUiPort, args.cores, args.memory, args.master)),
      name = "Worker")
    actorSystem.awaitTermination()
  }
}
