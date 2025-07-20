package org.globsframework.network;

import org.globsframework.core.model.Glob;

public interface GlobClient {
    Glob request(Glob data);
}
