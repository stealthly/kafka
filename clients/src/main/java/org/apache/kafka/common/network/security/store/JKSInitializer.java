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

package org.apache.kafka.common.network.security.store;

import org.apache.kafka.common.network.security.AuthConfig;
import org.apache.kafka.common.network.security.StoreInitializer;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

public class JKSInitializer implements StoreInitializer {
    public static final String NAME = "jks";
    private static JKSInitializer instance = null;

    private JKSInitializer() {
    }

    public static JKSInitializer getInstance() {
        if (instance == null) {
            synchronized (JKSInitializer.class) {
                if (instance == null) {
                    instance = new JKSInitializer();
                }
            }
        }

        return instance;
    }

    public SSLContext initialize(AuthConfig config) throws Exception {
        TrustManager[] trustManagers = getTrustManagers(config);
        KeyManager[] keyManagers = getKeyManagers(config);

        return initContext(trustManagers, keyManagers);
    }

    private TrustManager[] getTrustManagers(AuthConfig config) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        FileInputStream in = new FileInputStream(config.getTruststore());
        trustStore.load(in, config.getTruststorePassword().toCharArray());
        in.close();

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);

        return tmf.getTrustManagers();
    }

    private KeyManager[] getKeyManagers(AuthConfig config) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        FileInputStream in = new FileInputStream(config.getKeystore());
        keyStore.load(in, config.getKeystorePassword().toCharArray());
        in.close();

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore, (config.getKeyPassword() != null) ?
                config.getKeyPassword().toCharArray() :
                config.getKeystorePassword().toCharArray());

        return kmf.getKeyManagers();
    }

    private SSLContext initContext(TrustManager[] tms, KeyManager[] kms) throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext authContext = SSLContext.getInstance("TLS");
        authContext.init(kms, tms, null);
        return authContext;
    }
}

