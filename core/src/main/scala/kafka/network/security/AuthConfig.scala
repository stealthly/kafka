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

package kafka.network.security

import kafka.utils.VerifiableProperties
import kafka.utils.Utils
import kafka.utils.Logging

object AuthConfig {
  val DEFAULT_SECURITY_CONFIG = "config/client.security.properties"
}

class AuthConfig(var securityConfigFile: String) extends Logging {

  val props = {
    if (securityConfigFile == null) {
      warn("securityConfigFile is null, using default securityConfigFile %s".format(AuthConfig.DEFAULT_SECURITY_CONFIG))
      securityConfigFile = AuthConfig.DEFAULT_SECURITY_CONFIG
    }
    new VerifiableProperties(Utils.loadProps(securityConfigFile))
  }

  val keystoreType = props.getString("keystore.type")

  /** Request client auth */
  val wantClientAuth = props.getBoolean("want.client.auth", false)

  /** Require client auth */
  val needClientAuth = props.getBoolean("need.client.auth", false)

  /** Keystore file location */
  val keystore = props.getString("keystore")

  /** Keystore file password */
  val keystorePwd = props.getString("keystorePwd")

  /** Keystore key password */
  val keyPwd = props.getString("keyPwd")

  /** Truststore file location */
  val truststore = props.getString("truststore")

  /** Truststore file password */
  val truststorePwd = props.getString("truststorePwd")
}
