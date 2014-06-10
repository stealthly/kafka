package org.apache.kafka.common.network.security;

import org.apache.kafka.common.errors.UnknownKeyStoreException;
import org.apache.kafka.common.network.security.store.JKSInitializer;

/**
 * Created by ilyutov on 6/5/14.
 */
public class KeyStores {
    private KeyStores() {
    }

    public static StoreInitializer getKeyStore(String name) throws UnknownKeyStoreException {
        if (JKSInitializer.NAME.equals(name)) {
            return JKSInitializer.getInstance();
        } else {
            throw new UnknownKeyStoreException(String.format("%s is an unknown key store", name));
        }
    }
}
