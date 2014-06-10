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

package org.apache.kafka.common.network.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class AuthConfig {
    public static String DEFAULT_SECURITY_CONFIG = "config/client.security.config";
    private final Properties props;

    public AuthConfig(String securityConfigFile) throws IOException {
        if (securityConfigFile == null) {
            securityConfigFile = AuthConfig.DEFAULT_SECURITY_CONFIG;
        }
        props = new Properties();
        props.load(Files.newInputStream(Paths.get(securityConfigFile)));
    }

    public String getKeystoreType() {
        return props.getProperty("keystore.type");
    }

    public boolean wantClientAuth() {
        return Boolean.valueOf(props.getProperty("want.client.auth"));
    }

    public boolean needClientAuth() {
        return Boolean.valueOf(props.getProperty("need.client.auth"));
    }

    public String getKeystore() {
        return props.getProperty("keystore");
    }

    public String getKeystorePassword() {
        return props.getProperty("keystorePwd");
    }

    public String getKeyPassword() {
        return props.getProperty("keyPwd");
    }

    public String getTruststore() {
        return props.getProperty("truststore");
    }

    public String getTruststorePassword() {
        return props.getProperty("truststorePwd");
    }
}
