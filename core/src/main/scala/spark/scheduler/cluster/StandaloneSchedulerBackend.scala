package spark.scheduler.cluster

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import akka.util.duration._
import akka.pattern.ask

import spark.{SparkException, Logging, TaskState}
import akka.dispatch.Await
import java.util.concurrent.atomic.AtomicInteger

/**
 * A standalone scheduler backend, which waits for standalone executors to connect to it through
 * Akka. These may be executed in a variety of ways, such as Mesos tasks for the coarse-grained
 * Mesos mode or standalone processes for Spark's standalone deploy mode (spark.deploy.*).
 */
class StandaloneSchedulerBackend(scheduler: ClusterScheduler, actorSystem: ActorSystem)
  extends SchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)

  class MasterActor(sparkProperties: Seq[(String, String)]) extends Actor {
    val slaveActor = new HashMap[String, ActorRef]
    val slaveHost = new HashMap[String, String]
    val freeCores = new HashMap[String, Int]
    val rxBps = new HashMap[String, Double]
    val txBps = new HashMap[String, Double]

    def receive = {
      case RegisterSlave(slaveId, host, cores, curRxBps, curTxBps) =>
        if (slaveActor.contains(slaveId)) {
          sender ! RegisterSlaveFailed("Duplicate slave ID: " + slaveId)
        } else {
          logInfo("Registered slave: " + sender + " with ID " + slaveId + " CurRxBps = " + curRxBps + " CurTxBps = " + curTxBps)
          sender ! RegisteredSlave(sparkProperties)
          slaveActor(slaveId) = sender
          slaveHost(slaveId) = host
          freeCores(slaveId) = cores
          rxBps(slaveId) = curRxBps
          txBps(slaveId) = curTxBps
          totalCoreCount.addAndGet(cores)
          makeOffers()
        }

      case StatusUpdate(slaveId, taskId, state, data, curRxBps, curTxBps) =>
        logInfo("Slave: " + sender + " with ID " + slaveId + " CurRxBps = " + curRxBps + " CurTxBps = " + curTxBps)
        rxBps(slaveId) = curRxBps
        txBps(slaveId) = curTxBps
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          freeCores(slaveId) += 1
          makeOffers(slaveId)
        }

      case ReviveOffers =>
        makeOffers()

      case StopMaster =>
        sender ! true
        context.stop(self)

      // TODO: Deal with nodes disconnecting too! (Including decreasing totalCoreCount)
    }

    // Make fake resource offers on all slaves
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(
        slaveHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id), rxBps(id), txBps(id))}))
    }

    // Make fake resource offers on just one slave
    def makeOffers(slaveId: String) {
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(slaveId, slaveHost(slaveId), freeCores(slaveId), rxBps(slaveId), txBps(slaveId)))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        freeCores(task.slaveId) -= 1
        slaveActor(task.slaveId) ! LaunchTask(task)
      }
    }
  }

  var masterActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  def start() {
    val properties = new ArrayBuffer[(String, String)]
    val iterator = System.getProperties.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    masterActor = actorSystem.actorOf(
      Props(new MasterActor(properties)), name = StandaloneSchedulerBackend.ACTOR_NAME)
  }

  def stop() {
    try {
      if (masterActor != null) {
        val timeout = 5.seconds
        val future = masterActor.ask(StopMaster)(timeout)
        Await.result(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's master actor", e)
    }
  }

  def reviveOffers() {
    masterActor ! ReviveOffers
  }

  def defaultParallelism(): Int = math.max(totalCoreCount.get(), 2)
}

object StandaloneSchedulerBackend {
  val ACTOR_NAME = "StandaloneScheduler"
}
