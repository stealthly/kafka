package kafka.tools.testing

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.Collections

import joptsimple.OptionParser
import org.apache.kafka.common.PartitionReplicaAssignment
import org.apache.kafka.common.requests.admin.AlterTopicRequest
import org.apache.kafka.common.requests.admin.AlterTopicRequest.AlterTopicArguments

import scala.collection.{Map, mutable}
import scala.collection.JavaConverters._

/**
 * NOTE: for testing KIP-4 purposes only, should NOT be merged as part of KIP-4
 */
object AlterTopicTool {

  class AlterTopicOpts(args: Array[String]) {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "The name of the topic to be altered")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being altered")
      .withRequiredArg
      .describedAs("# of partitions")
      .ofType(classOf[java.lang.Integer])
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being altered")
      .withRequiredArg
      .describedAs("replication factor")
      .ofType(classOf[java.lang.Integer])
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being altered")
      .withRequiredArg
      .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
      "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
      .ofType(classOf[String])

    val options = parser.parse(args: _*)
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Command to alter-topic ./test-alter-topic.sh <path-to-file> <broker ips>")
      Runtime.getRuntime.halt(1)
    }

    val file = args(0)

    val brokers = args(1).split(",")
    val nodes = brokers.map {
      str =>
        new InetSocketAddress(str.split(":")(0).trim, str.split(":")(1).trim.toInt)
    }

    val lines = io.Source.fromFile(new File(file)).getLines()
    val argLines = lines.filterNot(l => l.startsWith("#")).map(_.trim).toList

    val adminClient = new AdminClient(nodes)
    val alterTopicArgs =
      argLines.map {
        argLine =>
          val alterTopicArg =
            try {
              parseToAlterTopicArg(argLine)
            } catch {
              case e: Exception =>
                println(s"Failed to parse $argLine. Error: ${e.getMessage}")
                e.printStackTrace()
                Runtime.getRuntime.halt(-1)
                null
            }

          alterTopicArg
      }

    println(s"About to send AlterTopicRequest with ${alterTopicArgs.size} underlying alter-topic commands")
    val response =
      adminClient.deleteTopics(new AlterTopicRequest(alterTopicArgs.toMap.asJava))

    println("Got the response:")
    println(response.errors().asScala.map { case (k, v) => k + " ->" + v }.mkString("\n"))

    adminClient.close()
  }

  private def parseToAlterTopicArg(argLine: String): (String, AlterTopicArguments) = {
    val opts = new AlterTopicOpts(argLine.split(" ").map(_.trim))
    val topic =
      if (opts.options.has(opts.topicOpt))
        opts.options.valueOf(opts.topicOpt)
      else throw new RuntimeException("--topic is required")


    val replicaAssignments: util.List[PartitionReplicaAssignment] =
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignments = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        convertAssignments(assignments)
      } else Collections.emptyList()

    val partitions =
      if (opts.options.has(opts.partitionsOpt))
        opts.options.valueOf(opts.partitionsOpt)
      else new Integer(-1)

    val replicationFactor =
      if (opts.options.has(opts.replicationFactorOpt))
        opts.options.valueOf(opts.replicationFactorOpt)
      else new Integer(-1)

    topic -> new AlterTopicArguments(partitions, replicationFactor, replicaAssignments)
  }

  private def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      ret.put(i, brokerList.toList)
    }
    ret.toMap
  }

  private def convertAssignments(assignments: Map[Int, List[Int]]): util.List[PartitionReplicaAssignment] = {
    assignments.map {
      case (partition, replicas) =>
        new PartitionReplicaAssignment(partition, replicas.map(i => new java.lang.Integer(i)).asJava)
    }.toSeq.asJava
  }

}