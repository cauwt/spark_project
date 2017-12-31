package com.ibeifeng.sparkproject.spark.session;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by zkpk on 12/31/17.
 */
public class ToKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(CategorySortKey.class, new FieldSerializer(kryo, CategorySortKey.class));
    }
}
