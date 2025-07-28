package org.globsframework.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.serialisation.glob.GlobInstantiator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobsCache implements GlobInstantiator {
    Map<GlobType, Deque<MutableGlob>> globs = new ConcurrentHashMap<>();

    public MutableGlob newGlob(GlobType globType) {
        final Deque<MutableGlob> compute = globs.compute(globType, (globType1, mutableGlobs) -> {
            if (mutableGlobs == null) {
                mutableGlobs = new ArrayDeque<>();
            }
            return mutableGlobs;
        });
        MutableGlob data = null;
        synchronized (compute) {
            data = compute.poll();
        }
        if (data == null) {
            return globType.instantiate();
        }
        return data;
    }

    public void release(Glob glob) {
        if (glob instanceof MutableGlob mutableGlob) {
            final GlobType type = glob.getType();
            for (Field field : type.getFields()) {
                mutableGlob.unset(field);
            }
            globs.computeIfPresent(type, (globType, mutableGlobs) -> {
                mutableGlobs.add(mutableGlob);
                return mutableGlobs;
            });
        }
    }
}
