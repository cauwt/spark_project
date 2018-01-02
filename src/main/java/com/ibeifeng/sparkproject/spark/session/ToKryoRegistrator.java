package com.ibeifeng.sparkproject.spark.session;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by zkpk on 12/31/17.
 */
public class ToKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(CategorySortKey.class, new FieldSerializer(kryo, CategorySortKey.class));
        kryo.register(IntList.class,  new FieldSerializer(kryo, IntList.class));
    }
}
