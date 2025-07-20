package org.globsframework.network;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.serialisation.model.FieldNumber;
import org.globsframework.serialisation.model.FieldNumber_;
import org.globsframework.serialisation.model.GlobTypeNumber;
import org.globsframework.serialisation.model.GlobTypeNumber_;

import java.util.List;

class DummyObject {
    @GlobTypeNumber_(1)
    public static final GlobType TYPE;

    @FieldNumber_(1)
    public static final LongField id;

    @FieldNumber_(2)
    public static final StringField name;

    static {
        final GlobTypeBuilder dummyObject = DefaultGlobTypeBuilder.init("DummyObject",
                List.of(GlobTypeNumber.create(1)));
        TYPE = dummyObject.unCompleteType();
        id = dummyObject.declareLongField("id", FieldNumber.create(1));
        name = dummyObject.declareStringField("name", FieldNumber.create(2));
        dummyObject.complete();
    }
}
