package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.core.utils.serialization.SerializedInputOutputFactory;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;

import java.util.Base64;

public class Dump {
    public static void main(String[] args) {
        dump("AAAAAAAAAAEAAEBiAAAAKwAAAAIAAAAmAACkwAAAAE4AAAAHYmxhIGJsYQAAAGgAAOhdsemUfgAAAAM=");

        dump("AAAAAAAAAAEAAEBjAAAAKwAAAAIAAAAmAABWngAAAE4AAAAHYmxhIGJsYQAAAGgAAOhdsiGrmAAAAAM=");

    }

    private static void dump(String str2) {
        final byte[] decode = Base64.getDecoder().decode(str2);
        SerializedInput serializedInput = SerializedInputOutputFactory.init(decode);

        BinReaderFactory binReader = BinReaderFactory.create();
        BinReader globBinReader = binReader.createGlobBinReader(GlobType::instantiate, serializedInput);

        System.out.println("streamId=" + serializedInput.readNotNullLong());
        System.out.println("requestId=" + serializedInput.readNotNullInt());
        System.out.println("size=" + serializedInput.readNotNullInt());
        System.out.println("data=" + globBinReader.read(ExchangeData.TYPE));
    }

}
