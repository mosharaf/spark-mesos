package spark.scheduler.cluster

import spark.TaskState.TaskState
import java.nio.ByteBuffer
import spark.util.SerializableBuffer

sealed trait StandaloneClusterMessage extends Serializable

// Master to slaves
case class LaunchTask(task: TaskDescription) extends StandaloneClusterMessage
case class RegisteredSlave(sparkProperties: Seq[(String, String)]) extends StandaloneClusterMessage
case class RegisterSlaveFailed(message: String) extends StandaloneClusterMessage

// Slaves to master
case class RegisterSlave(slaveId: String, host: String, cores: Int, curRxBps: Double, curTxBps: Double) extends StandaloneClusterMessage

case class StatusUpdate(slaveId: String, taskId: Long, state: TaskState, data: SerializableBuffer, curRxBps: Double, curTxBps: Double)
  extends StandaloneClusterMessage

object StatusUpdate {
  /** Alternate factory method that takes a ByteBuffer directly for the data field */
  def apply(slaveId: String, taskId: Long, state: TaskState, data: ByteBuffer, curRxBps: Double, curTxBps: Double): StatusUpdate = {
    StatusUpdate(slaveId, taskId, state, new SerializableBuffer(data), curRxBps, curTxBps)
  }
}

// Internal messages in master
case object ReviveOffers extends StandaloneClusterMessage
case object StopMaster extends StandaloneClusterMessage
