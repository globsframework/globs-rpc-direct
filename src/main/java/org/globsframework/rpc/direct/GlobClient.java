package org.globsframework.rpc.direct;

import org.globsframework.core.model.Glob;

public interface GlobClient {
    Glob request(Glob data);
}
