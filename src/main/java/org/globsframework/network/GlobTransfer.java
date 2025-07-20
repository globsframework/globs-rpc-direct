package org.globsframework.network;

import org.globsframework.core.model.Glob;

/**
 * A utility class to transfer Globs between client and server for testing purposes.
 * In a real implementation, you would use proper serialization/deserialization.
 */
public class GlobTransfer {
    private static final ThreadLocal<Glob> CURRENT_GLOB = new ThreadLocal<>();

    public static void setCurrentGlob(Glob glob) {
        CURRENT_GLOB.set(glob);
    }

    public static Glob getCurrentGlob() {
        return CURRENT_GLOB.get();
    }

    public static void clear() {
        CURRENT_GLOB.remove();
    }
}