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

package kafka.network.security

import javax.net.ssl.SSLContext
import kafka.utils.Logging
import java.util.concurrent.atomic.AtomicBoolean

object SecureAuth extends Logging {
  private val initialized = new AtomicBoolean(false)
  private var authContext: SSLContext = null

  def isInitialized = initialized.get

  def sslContext = {
    if (!initialized.get) {
      throw new IllegalStateException("Secure authentication is not initialized.")
    }
    authContext
  }

  def initialize(config: AuthConfig) {
    if (initialized.get) {
      warn("Attempt to reinitialize auth context")
      return
    }

    info("Initializing secure authentication")

    val initializer = KeyStores.getKeyStore(config.keystoreType)
    authContext = initializer.initialize(config)

    initialized.set(true)

    info("Secure authentication initialization has been successfully completed")
  }
}
