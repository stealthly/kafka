package kafka.tools.testing

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.{Collections, Properties}

import joptsimple.OptionParser
import org.apache.kafka.common.requests.admin.CreateTopicRequest
import org.apache.kafka.common.{PartitionReplicaAssignment, ConfigEntry}
import org.apache.kafka.common.requests.admin.CreateTopicRequest.CreateTopicArguments

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

/**
 * NOTE: for testing KIP-4 purposes only, should NOT be merged as part of KIP-4
 */
object CreateTopicTool {

  class CreateTopicOpts(args: Array[String]) {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "The name of the topic to be created")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created")
      .withRequiredArg
      .describedAs("# of partitions")
      .ofType(classOf[java.lang.Integer])
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created")
      .withRequiredArg
      .describedAs("replication factor")
      .ofType(classOf[java.lang.Integer])
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created")
      .withRequiredArg
      .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
      "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
      .ofType(classOf[String])

    val configOpt = parser.accepts("config", "A configuration override for the topic being created")
      .withRequiredArg
      .describedAs("name=value")
      .ofType(classOf[String])

    val options = parser.parse(args: _*)
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Command to create-topic ./test-create-topic.sh <path-to-file> <broker ips>")
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
    val createTopicArgs =
      argLines.map {
        argLine =>
          val createTopicArg =
            try {
              parseToCreateTopicArg(argLine)
            } catch {
              case e: Exception =>
                println(s"Failed to parse $argLine. Error: ${e.getMessage}")
                e.printStackTrace()
                Runtime.getRuntime.halt(-1)
                null
            }

          createTopicArg
      }

    println(s"About to send CreateTopicRequest with ${createTopicArgs.size} underlying create-topic commands")
    val response =
      adminClient.createTopics(new CreateTopicRequest(createTopicArgs.toMap.asJava))

    println("Got the response:")
    println(response.errors().asScala.map { case (k, v) => k + " ->" + v }.mkString("\n"))

    adminClient.close()
  }

  private def parseToCreateTopicArg(argLine: String): (String, CreateTopicArguments) = {
    val opts = new CreateTopicOpts(argLine.split(" ").map(_.trim))
    val topic =
      if (opts.options.has(opts.topicOpt))
        opts.options.valueOf(opts.topicOpt)
      else throw new RuntimeException("--topic is required")

    val configs =
      if (opts.options.has(opts.configOpt))
        parseTopicConfigsToBeAdded(opts.options.valuesOf(opts.configOpt).asScala)
      else Nil

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

    topic -> new CreateTopicArguments(partitions, replicationFactor, replicaAssignments, configs.asJava)
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

  private def parseTopicConfigsToBeAdded(configs: Seq[String]): Seq[ConfigEntry] = {
    configs.map(c =>
      c.split( """\s*=\s*""") match {
        case Array(k, v) => new ConfigEntry(k, v)
        case _ => throw new RuntimeException("Invalid topic config: all configs to be added must be in the format \"key=val\".")
      }
    )
  }

  private def convertAssignments(assignments: Map[Int, List[Int]]): util.List[PartitionReplicaAssignment] = {
    assignments.map {
      case (partition, replicas) =>
        new PartitionReplicaAssignment(partition, replicas.map(i => new java.lang.Integer(i)).asJava)
    }.toSeq.asJava
  }

}