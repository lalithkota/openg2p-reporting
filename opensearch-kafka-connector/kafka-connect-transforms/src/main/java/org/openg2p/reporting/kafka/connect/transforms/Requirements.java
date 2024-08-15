package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class Requirements {

    public static void requireSchema(Schema schema, String purpose) {
        if (schema == null) {
            throw new DataException("Schema required for [" + purpose + "]");
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> requireMap(Object value, String purpose) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Map<String, Object>) value;
    }

    public static Map<String, Object> requireMapOrNull(Object value, String purpose) {
        if (value == null) {
            return null;
        }
        return requireMap(value, purpose);
    }

    public static Struct requireStruct(Object value, String purpose) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    public static Struct requireStructOrNull(Object value, String purpose) {
        if (value == null) {
            return null;
        }
        return requireStruct(value, purpose);
    }

    public static SinkRecord requireSinkRecord(ConnectRecord<?> record, String purpose) {
        if (!(record instanceof SinkRecord)) {
            throw new DataException("Only SinkRecord supported for [" + purpose + "], found: " + nullSafeClassName(record));
        }
        return (SinkRecord) record;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }

    public static Object getNestedField(Map<String,Object> tree, String field){
        Object v=tree;
        for(String subfield: field.split("\\.")){
            v = ((Map<String,Object>)v).get(subfield);
            if(v == null){
                break;
            }
        }
        return v;
    }

    public static Object[] getNestedField(Struct tree, String field){
        // First value of list returned is the value
        // and second is the Schema of the value

        Object[] ret = new Object[2];

        Schema vSchema = tree.schema();
        for(String subfield: field.split("\\.")){
            try{
                ret[0] = tree.get(subfield);
                ret[1] = (vSchema = vSchema.field(subfield).schema());
                tree = tree.getStruct(subfield);
            } catch(DataException de){ return ret; }
        }
        return ret;
    }
}
