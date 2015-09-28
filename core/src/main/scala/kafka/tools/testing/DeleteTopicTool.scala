package kafka.tools.testing

import java.io.File
import java.net.InetSocketAddress

import joptsimple.OptionParser
import org.apache.kafka.common.requests.admin.DeleteTopicRequest

import scala.collection.JavaConverters._

object DeleteTopicTool {

  class DeleteTopicOpts(args: Array[String]) {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "The name of the topic to be delete")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val options = parser.parse(args: _*)
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Command to delete-topic ./test-delete-topic.sh <path-to-file> <broker ips>")
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
    val deleteTopicArgs =
      argLines.map {
        argLine =>
          val topicToBeDeleted =
            try {
              parseTopic(argLine)
            } catch {
              case e: Exception =>
                println(s"Failed to parse $argLine. Error: ${e.getMessage}")
                e.printStackTrace()
                Runtime.getRuntime.halt(-1)
                null
            }

          topicToBeDeleted
      }

    println(s"About to send DeleteTopicRequest with ${deleteTopicArgs.size} underlying delete-topic commands")
    val response =
      adminClient.deleteTopics(new DeleteTopicRequest(deleteTopicArgs.toSet.asJava))

    println("Got the response:")
    println(response.errors().asScala.map { case (k, v) => k + " ->" + v }.mkString("\n"))

    adminClient.close()
  }

  private def parseTopic(argLine: String): String = {
    val opts = new DeleteTopicOpts(argLine.split(" ").map(_.trim))
    val topic =
      if (opts.options.has(opts.topicOpt))
        opts.options.valueOf(opts.topicOpt)
      else throw new RuntimeException("--topic is required")

    topic
  }
}
