package org.apache.kafka.common.network.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.concurrent.atomic.AtomicBoolean;

public class SecureAuth {
    private static final Logger log = LoggerFactory.getLogger(SecureAuth.class);
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static SSLContext authContext;

    private SecureAuth() {
    }

    public static SSLContext getSSLContext() {
        if (!initialized.get()) {
            throw new IllegalStateException("Secure authentication is not initialized.");
        }
        return authContext;
    }

    public static void initialize(AuthConfig config) throws Exception {
        if (initialized.get()) {
            log.warn("Attempt to reinitialize auth context");
            return;
        }

        log.info("Initializing secure authentication");

        StoreInitializer initializer = KeyStores.getKeyStore(config.getKeystoreType());
        authContext = initializer.initialize(config);

        initialized.set(true);

        log.info("Secure authentication initialization has been successfully completed");
    }
}

