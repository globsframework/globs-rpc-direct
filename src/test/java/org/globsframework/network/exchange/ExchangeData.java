package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.serialisation.model.FieldNumber;

public class ExchangeData {
    public static final GlobType TYPE;

    public static final IntegerField groupId;

    public static final IntegerField id;

    public static final StringField DATA;

    public static final LongField sendAtNS;

    static {
        final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Exchange");
        TYPE = typeBuilder.unCompleteType();
        groupId = typeBuilder.declareIntegerField("groupId", FieldNumber.create(4));
        id = typeBuilder.declareIntegerField("id", FieldNumber.create(1));
        DATA = typeBuilder.declareStringField("data", FieldNumber.create(2));
        sendAtNS = typeBuilder.declareLongField("updatedAt", FieldNumber.create(3));
        typeBuilder.complete();
    }

    public static Glob create(String data, int id, long createdAt) {
        return TYPE.instantiate()
                .set(DATA, data)
                .set(ExchangeData.id, id)
                .set(sendAtNS, createdAt);
    }

    public static Glob create(String data, int id, long createdAt, int groupId) {
        return TYPE.instantiate()
                .set(ExchangeData.groupId, groupId)
                .set(DATA, data)
                .set(ExchangeData.id, id)
                .set(sendAtNS, createdAt);
    }

}
