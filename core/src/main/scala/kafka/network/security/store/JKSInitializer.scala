/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network.security.store

import java.io.FileInputStream
import javax.net.ssl._
import kafka.network.security.{StoreInitializer, AuthConfig}

object JKSInitializer extends StoreInitializer {
  val name = "jks"

  def initialize(config: AuthConfig) = {
    val tms = config.truststorePwd match {
      case pw: String =>
        val ts = java.security.KeyStore.getInstance("JKS")
        val fis: FileInputStream = new FileInputStream(config.truststore)
        ts.load(fis, pw.toCharArray)
        fis.close()

        val tmf = TrustManagerFactory.getInstance("SunX509")
        tmf.init(ts)
        tmf.getTrustManagers
      case _ => null
    }
    val kms = config.keystorePwd match {
      case pw: String =>
        val ks = java.security.KeyStore.getInstance("JKS")
        val fis: FileInputStream = new FileInputStream(config.keystore)
        ks.load(fis, pw.toCharArray)
        fis.close()

        val kmf = KeyManagerFactory.getInstance("SunX509")
        kmf.init(ks, if (config.keyPwd != null) config.keyPwd.toCharArray else pw.toCharArray)
        kmf.getKeyManagers
      case _ => null
    }

    initContext(tms, kms)
  }

  private def initContext(tms: Array[TrustManager], kms: Array[KeyManager]): SSLContext = {
    val authContext = SSLContext.getInstance("TLS")
    authContext.init(kms, tms, null)
    authContext
  }
}
