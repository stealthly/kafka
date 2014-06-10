package org.apache.kafka.common.network.security;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * Created by ilyutov on 6/5/14.
 */
public interface StoreInitializer {
    SSLContext initialize(AuthConfig config) throws Exception;
}