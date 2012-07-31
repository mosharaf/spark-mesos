package spark.deploy

import spark.deploy.ExecutorState.ExecutorState

sealed trait DeployMessage extends Serializable

// Worker to Master

case class RegisterWorker(id: String, host: String, port: Int, cores: Int, memory: Int)
  extends DeployMessage

case class UpdateNetworkLoad(id: String, rxBps: Double, txBps: Double)
  extends DeployMessage

case class ExecutorStateChanged(
    jobId: String,
    execId: Int,
    state: ExecutorState,
    message: Option[String])
  extends DeployMessage

// Master to Worker

case object RegisteredWorker extends DeployMessage
case class RegisterWorkerFailed(message: String) extends DeployMessage
case class KillExecutor(jobId: String, execId: Int) extends DeployMessage

case class LaunchExecutor(
    jobId: String,
    execId: Int,
    jobDesc: JobDescription,
    cores: Int,
    memory: Int)
  extends DeployMessage


// Client to Master

case class RegisterJob(jobDescription: JobDescription) extends DeployMessage

// Master to Client

case class RegisteredJob(jobId: String) extends DeployMessage
case class ExecutorAdded(id: Int, workerId: String, host: String, cores: Int, memory: Int)
case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String])
case class JobKilled(message: String)

// Internal message in Client

case object StopClient