package org.globsframework.rpc.direct;

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

    @FieldNumber_(3)
    public static final LongField sendAt;

    @FieldNumber_(4)
    public static final LongField receivedAt;

    static {
        final GlobTypeBuilder dummyObject = DefaultGlobTypeBuilder.init("DummyObject",
                List.of(GlobTypeNumber.create(1)));
        TYPE = dummyObject.unCompleteType();
        id = dummyObject.declareLongField("id", FieldNumber.create(1));
        name = dummyObject.declareStringField("name", FieldNumber.create(2));
        sendAt = dummyObject.declareLongField("sendAt", FieldNumber.create(3));
        receivedAt = dummyObject.declareLongField("receivedAt", FieldNumber.create(4));
        dummyObject.complete();
    }
}
