package kafka.tools.testing

import java.net.InetSocketAddress
import java.util
import java.util.Collections

import org.apache.kafka.clients.{ClientRequest, ClientResponse, Metadata, NetworkClient}
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.config.SSLConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.admin._
import org.apache.kafka.common.requests.{AbstractRequestResponse, RequestSend}
import org.apache.kafka.common.utils.SystemTime

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * NOTE: for testing KIP-4 purposes only, shouldnot be merged as part of KIP-4
 */
class AdminClient(nodes: Seq[InetSocketAddress]) {

  private val time = new SystemTime()
  private val metadata = initMetadata()
  private val networkClient = initNetworkClient()

  private def initMetadata(): Metadata = {
    val metadata = new Metadata()
    metadata.update(Cluster.bootstrap(nodes.toList.asJava), time.milliseconds())

    metadata
  }

  private def initNetworkClient(): NetworkClient = {
    val secConfig = new util.HashMap[String, Object]()
    secConfig.put(SSLConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.security.auth.DefaultPrincipalBuilder"))

    val selector = new Selector(5000, new Metrics(), time, "admin-client", Collections.emptyMap(),
      ChannelBuilders.create(SecurityProtocol.PLAINTEXT, null, secConfig))

    new NetworkClient(selector, metadata, "adminClient", 10, 10L, 128 * 1024, 32 * 1024)
  }

  private def sendBlockingRequest(request: AbstractRequestResponse, apiKeys: ApiKeys): ClientResponse = {
    val requestSend = new RequestSend(
      metadata.fetch().nodes().get(0).id().toString,
      networkClient.nextRequestHeader(apiKeys),
      request.toStruct)

    networkClient.ready(metadata.fetch().nodes().get(0), time.milliseconds())
    networkClient.poll(2000, time.milliseconds())
    networkClient.send(new ClientRequest(time.milliseconds(), true, requestSend, null))

    val responses = networkClient.poll(10000L, time.milliseconds())

    val response =
      if (responses.size() > 0)
        responses.get(0)
      else
        networkClient.completeAll(time.milliseconds()).get(0)

    response
  }

  def createTopics(request: CreateTopicRequest): CreateTopicResponse = {
    val response = sendBlockingRequest(request, ApiKeys.CREATE_TOPIC)

    new CreateTopicResponse(response.responseBody())
  }

  def deleteTopics(request: AlterTopicRequest): AlterTopicResponse = {
    val response = sendBlockingRequest(request, ApiKeys.ALTER_TOPIC)

    new AlterTopicResponse(response.responseBody())
  }

  def deleteTopics(request: DeleteTopicRequest): DeleteTopicResponse = {
    val response = sendBlockingRequest(request, ApiKeys.DELETE_TOPIC)

    new DeleteTopicResponse(response.responseBody())
  }

  def close(): Unit = {
    Try(networkClient.close())
  }
}
