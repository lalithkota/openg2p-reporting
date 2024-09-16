package org.openg2p.reporting.kafka.connect.transforms.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StructMerger {
    public static enum MapMergeStrategy {
        deep,
        replace,
    }

    public static enum ArrayMergeStrategy {
        concat,
        replace,
    }

    public static Struct mergeStructs(MapMergeStrategy mapMergeStrategy, ArrayMergeStrategy arrMergeStrategy, Struct... structs) {
        if(structs.length < 1) return null;
        Struct res = structs[0];
        for (int i=1; i < structs.length; i++){
            res = mergeStructs(res, structs[i], mapMergeStrategy, arrMergeStrategy);
        }
        return res;
    }

    public static Struct mergeStructs(Struct struct1, Struct struct2, MapMergeStrategy mapMergeStrategy, ArrayMergeStrategy arrMergeStrategy) {
        if(struct1 == null) return struct2;
        if(struct2 == null) return struct1;
        Struct mergedStruct = new Struct(mergeSchemas(struct1.schema(), struct2.schema(), mapMergeStrategy, arrMergeStrategy));

        for (Field field : mergedStruct.schema().fields()) {
            Object value1 = null;
            Object value2 = null;
            if(struct1.schema().field(field.name()) != null) value1 = struct1.getWithoutDefault(field.name());
            if(struct2.schema().field(field.name()) != null) value2 = struct2.getWithoutDefault(field.name());

            if (value2 != null) {
                if (value1 == null) {
                    mergedStruct.put(field, value2);
                } else if (value1 instanceof Struct && value2 instanceof Struct && mapMergeStrategy == MapMergeStrategy.deep) {
                    mergedStruct.put(field, mergeStructs((Struct) value1, (Struct) value2, mapMergeStrategy, arrMergeStrategy));
                } else if (value1.getClass().isArray() && value1.getClass().equals(value2.getClass()) && arrMergeStrategy == ArrayMergeStrategy.concat) {
                    mergedStruct.put(field, concatArrays(value1, value2, value2.getClass()));
                } else if (value1 instanceof Map && value2 instanceof Map && mapMergeStrategy == MapMergeStrategy.deep) {
                    mergedStruct.put(field, mergeMaps((Map<String, Object>) value1, (Map<String, Object>) value2, mapMergeStrategy, arrMergeStrategy));
                } else if (value1 instanceof List && value2 instanceof List && arrMergeStrategy == ArrayMergeStrategy.concat) {
                    mergedStruct.put(field, Stream.concat(((List)value1).stream(), ((List)value2).stream()).collect(Collectors.toList()));
                } else {
                    mergedStruct.put(field, value2);
                }
            } else {
                mergedStruct.put(field, value1);
            }
        }

        return mergedStruct;
    }

    public static Map<String, Object> mergeMaps(MapMergeStrategy mapMergeStrategy, ArrayMergeStrategy arrMergeStrategy, Map<String, Object>... maps) {
        if(maps.length < 1) return null;
        Map<String, Object> res = maps[0];
        for (int i=1; i < maps.length; i++){
            res = mergeMaps(res, maps[i], mapMergeStrategy, arrMergeStrategy);
        }
        return res;
    }

    public static Map<String, Object> mergeMaps(Map<String, Object> map1, Map<String, Object> map2, MapMergeStrategy mapMergeStrategy, ArrayMergeStrategy arrMergeStrategy) {
        if (map1 == null) return map2;
        if (map2 == null) return map1;
        for(String field: map1.keySet()) {
            Object value1 = map1.get(field);
            Object value2 = map2.get(field);
            if(!map2.containsKey(field)) {
                map2.put(field, value1);
            } else {
                if (value1 instanceof Map && value2 instanceof Map && mapMergeStrategy == MapMergeStrategy.deep) {
                    mergeMaps((Map<String, Object>)value1, (Map<String, Object>)value2, mapMergeStrategy, arrMergeStrategy);
                } else if (value1.getClass().isArray() && value2.getClass().isArray() && arrMergeStrategy == ArrayMergeStrategy.concat) {
                    map2.put(field, concatArrays(value1, value2, value1.getClass()));
                } else if (value1 instanceof List && value2 instanceof List && arrMergeStrategy == ArrayMergeStrategy.concat) {
                    map2.put(field, Stream.concat(((List)value1).stream(), ((List)value2).stream()).collect(Collectors.toList()));
                }
            }
        }
        return map2;
    }

    @SuppressWarnings("unchecked")
    public static <T> T concatArrays(Object array1, Object array2, Class<T> clazz) {
        // T must be an array class
        assert clazz.isArray();
        if ((array1 == null || Array.getLength(array1) == 0) && (array2 == null || Array.getLength(array2) == 0)) return null;
        else if ((array1 == null || Array.getLength(array1) == 0) && array2 != null) return clazz.cast(array2);
        else if (array1 != null && (array2 == null || Array.getLength(array2) == 0)) return clazz.cast(array1);
        T value1 = clazz.cast(array1);
        T value2 = clazz.cast(array2);
        T merged = (T)Array.newInstance(clazz.getComponentType(), Array.getLength(value1) + Array.getLength(value2));
        System.arraycopy(value1, 0, merged, 0, Array.getLength(value1));
        System.arraycopy(value2, 0, merged, Array.getLength(value1), Array.getLength(value2));
        return merged;
    }

    public static Schema mergeSchemas(Schema schema1, Schema schema2, MapMergeStrategy mapMergeStrategy, ArrayMergeStrategy arrMergeStrategy) {
        if (schema1 == null) return schema2;
        if (schema2 == null) return schema1;
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema2);
        for (Field field: schema2.fields()) {
            builder.field(field.name(), field.schema());
        }
        for (Field field: schema1.fields()){
            Field schema1Field = field;
            Field schema2Field = schema2.field(field.name());
            if(schema2Field == null){
                // This means schema1Field is not present in schema2.
                builder.field(field.name(), field.schema());
            } else {
                if (schema1Field.schema().type() == Schema.Type.STRUCT && schema2Field.schema().type() == Schema.Type.STRUCT && mapMergeStrategy == MapMergeStrategy.deep) {
                    builder.field(field.name(), mergeSchemas(schema1Field.schema(), schema2Field.schema(), mapMergeStrategy, arrMergeStrategy));
                }
            }
        }
        return builder.build();
    }
}
