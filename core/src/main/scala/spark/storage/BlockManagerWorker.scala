package spark.storage

import java.nio._

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Random

import spark.Logging
import spark.Utils
import spark.SparkEnv
import spark.network._

/**
 * This should be changed to use event model late. 
 */
class BlockManagerWorker(val blockManager: BlockManager) extends Logging {
  initLogging()
  
  blockManager.connectionManager.onReceiveMessage(onBlockMessageReceive)

  def onBlockMessageReceive(msg: Message, id: ConnectionManagerId): Option[Message] = {
    logDebug("Handling message " + msg)
    msg match {
      case bufferMessage: BufferMessage => {
        try {
          logDebug("Handling as a buffer message " + bufferMessage)
          val blockMessages = BlockMessageArray.fromBufferMessage(bufferMessage)
          logDebug("Parsed as a block message array")
          val responseMessages = blockMessages.map(processBlockMessage _).filter(_ != None).map(_.get)
          /*logDebug("Processed block messages")*/
          return Some(new BlockMessageArray(responseMessages).toBufferMessage)
        } catch {
          case e: Exception => logError("Exception handling buffer message: " + e.getMessage)
          return None
        }
      }
      case otherMessage: Any => {
        logError("Unknown type message received: " + otherMessage)
        return None
      }
    }
  }

  def processBlockMessage(blockMessage: BlockMessage): Option[BlockMessage] = {
    blockMessage.getType match {
      case BlockMessage.TYPE_REPLICATE_BLOCK => {
        val rB = ReplicateBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel, blockMessage.getPeers)
        logInfo("Received [" + rB + "]")
        putBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
        if (!BlockManagerWorker.syncReplicateBlock(rB)) {
          logError("Failed to call syncReplicateBlock to " + rB.peers)
        }
        return None
      }
      case BlockMessage.TYPE_PUT_BLOCK => {
        val pB = PutBlock(blockMessage.getId, blockMessage.getData, blockMessage.getLevel)
        logInfo("Received [" + pB + "]")
        putBlock(pB.id, pB.data, pB.level)
        return None
      } 
      case BlockMessage.TYPE_GET_BLOCK => {
        val gB = new GetBlock(blockMessage.getId)
        logInfo("Received [" + gB + "]")
        val buffer = getBlock(gB.id)
        if (buffer == null) {
          return None
        }
        return Some(BlockMessage.fromGotBlock(GotBlock(gB.id, buffer)))
      }
      case _ => return None
    }
  }

  private def putBlock(id: String, bytes: ByteBuffer, level: StorageLevel) {
    val startTimeMs = System.currentTimeMillis()
    logDebug("PutBlock " + id + " started from " + startTimeMs + " with data: " + bytes)
    blockManager.putBytes(id, bytes, level)
    logDebug("PutBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " with data size: " + bytes.array().length)
  }

  private def getBlock(id: String): ByteBuffer = {
    val startTimeMs = System.currentTimeMillis()
    logDebug("Getblock " + id + " started from " + startTimeMs)
    val block = blockManager.getLocal(id)
    val buffer = block match {
      case Some(tValues) => {
        val values = tValues
        val buffer = blockManager.dataSerialize(values)
        buffer
      }
      case None => { 
        null
      }
    }
    logDebug("GetBlock " + id + " used " + Utils.getUsedTimeMs(startTimeMs)
        + " and got buffer " + buffer)
    return buffer
  }
}

object BlockManagerWorker extends Logging {
  private var blockManagerWorker: BlockManagerWorker = null
  private val DATA_TRANSFER_TIME_OUT_MS: Long = 500
  private val REQUEST_RETRY_INTERVAL_MS: Long = 1000
  
  initLogging()
  
  def startBlockManagerWorker(manager: BlockManager) {
    blockManagerWorker = new BlockManagerWorker(manager)
  }

  def syncReplicateBlock(msg: ReplicateBlock): Boolean = {
    if (msg.peers.length == 0)
      return true

    val blockManager = blockManagerWorker.blockManager
    val connectionManager = blockManager.connectionManager
    val serializer = blockManager.serializer
    val next = msg.peers.head
    val toConnManagerId = new ConnectionManagerId(next.ip, next.port)
    val newPeers = msg.peers.takeRight(msg.peers.length - 1)
    val blockMessage = BlockMessage.fromReplicateBlock(ReplicateBlock(msg.id, msg.data, msg.level, newPeers))
    val blockMessageArray = new BlockMessageArray(blockMessage)
    val resultMessage = connectionManager.sendMessageReliablySync(
        toConnManagerId, blockMessageArray.toBufferMessage)
    return (resultMessage != None)
  }
  
  def syncPutBlock(msg: PutBlock, toConnManagerId: ConnectionManagerId): Boolean = {
    val blockManager = blockManagerWorker.blockManager
    val connectionManager = blockManager.connectionManager 
    val serializer = blockManager.serializer
    val blockMessage = BlockMessage.fromPutBlock(msg)
    val blockMessageArray = new BlockMessageArray(blockMessage)
    val resultMessage = connectionManager.sendMessageReliablySync(
        toConnManagerId, blockMessageArray.toBufferMessage)
    return (resultMessage != None)
  }
  
  def syncGetBlock(msg: GetBlock, toConnManagerId: ConnectionManagerId): ByteBuffer = {
    val blockManager = blockManagerWorker.blockManager
    val connectionManager = blockManager.connectionManager 
    val serializer = blockManager.serializer
    val blockMessage = BlockMessage.fromGetBlock(msg)
    val blockMessageArray = new BlockMessageArray(blockMessage)
    val responseMessage = connectionManager.sendMessageReliablySync(
        toConnManagerId, blockMessageArray.toBufferMessage)
    responseMessage match {
      case Some(message) => {
        val bufferMessage = message.asInstanceOf[BufferMessage]
        logDebug("Response message received " + bufferMessage)
        BlockMessageArray.fromBufferMessage(bufferMessage).foreach(blockMessage => {
            logDebug("Found " + blockMessage)
            return blockMessage.getData
          })
      }
      case None => logDebug("No response message received"); return null
    }
    return null
  }
}
