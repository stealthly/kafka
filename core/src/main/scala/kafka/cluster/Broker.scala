/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import kafka.utils.Utils._
import kafka.utils.Json
import kafka.api.ApiUtils._
import java.nio.ByteBuffer
import kafka.common.{KafkaException, BrokerNotAvailableException}

/**
 * A Kafka broker
 */
private[kafka] object Broker {

  def createBroker(id: Int, brokerInfoString: String): Broker = {
    if(brokerInfoString == null)
      throw new BrokerNotAvailableException("Broker id %s does not exist".format(id))
    try {
      Json.parseFull(brokerInfoString) match {
        case Some(m) =>
          val brokerInfo = m.asInstanceOf[Map[String, Any]]
          val host = brokerInfo.get("host").get.asInstanceOf[String]
          val port = brokerInfo.get("port").get.asInstanceOf[Int]
          val secure = if (brokerInfo.get("secure").get.asInstanceOf[Int] == 0) false else true
          new Broker(id, host, port, secure)
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(id))
      }
    } catch {
      case t: Throwable => throw new KafkaException("Failed to parse the broker info from zookeeper: " + brokerInfoString, t)
    }
  }

  def readFrom(buffer: ByteBuffer): Broker = {
    val id = buffer.getInt
    val host = readShortString(buffer)
    val port = buffer.getInt
    val secure = buffer.getShort == 1
    new Broker(id, host, port, secure)
  }
}

private[kafka] case class Broker(val id: Int, val host: String, val port: Int, val secure: Boolean = false) {
  
  override def toString(): String = new String("id:" + id + ",host:" + host + ",port:" + port + ",secure:" + secure)

  def getConnectionString(): String = host + ":" + port + ":" + (if (secure) 1 else 0)

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(id)
    writeShortString(buffer, host)
    buffer.putInt(port)
    buffer.putShort(if (secure) 1 else 0)
  }

  def sizeInBytes: Int = shortStringLength(host) /* host name */ + 4 /* port */ + 4 /* broker id*/ + 2 /* secure */

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: Broker => id == n.id && host == n.host && port == n.port && secure == n.secure
      case _ => false
    }
  }
  
  override def hashCode(): Int = hashcode(id, host, port, if (secure) 1 else 0)
  
}
