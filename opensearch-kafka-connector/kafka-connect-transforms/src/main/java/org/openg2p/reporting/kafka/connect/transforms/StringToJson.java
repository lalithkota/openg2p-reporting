package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.JSONObject;
import org.json.JSONArray;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

public abstract class StringToJson<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends StringToJson<R>{}
    public static class Value<R extends ConnectRecord<R>> extends StringToJson<R>{}

    public static final String PURPOSE = "String to json converter";
    public static final String INPUT_FIELD_CONFIG = "input.field";

    private String configInputField;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(INPUT_FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "The field that has to be converted to json/struct");

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema,Schema>(16));

        configInputField = absconf.getString(INPUT_FIELD_CONFIG);

        if(configInputField.isEmpty()){
            throw new ConfigException(INPUT_FIELD_CONFIG + " is not set.");
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        if(value.get(configInputField) != null && value.get(configInputField) instanceof String)
            updatedValue.put(configInputField,returnSchemalessObject(new JSONObject((String)value.get(configInputField))));

        return newRecord(record, null, updatedValue);
    }

    @Override
    public R applyWithSchema(R record) {
        final Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if(field.name().equals(configInputField)){
                if(field.schema()==Schema.STRING_SCHEMA || field.schema()==Schema.OPTIONAL_STRING_SCHEMA)
                    updatedValue.put(field.name(), returnSchemaObject(new JSONObject((String)value.get(field))));
            }
            else
                updatedValue.put(field.name(), value.get(field));
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    public Schema makeUpdatedSchema(Schema schema) {
        // this is not ready
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if(field.name().equals(configInputField))
                builder.field(field.name(), Schema.STRING_SCHEMA);
            else
                builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    static Object returnSchemalessObject(Object obj){
        if (obj instanceof JSONObject){
            Map<String, Object> returner = new HashMap<>();
            JSONObject json = (JSONObject)obj;
            Iterator<String> keys = json.keys();
            while(keys.hasNext()) {
                String key = keys.next();
                returner.put(key, returnSchemalessObject(json.get(key)));
            }
            return returner;
        }
        else if(obj instanceof JSONArray){
            JSONArray arr = (JSONArray)obj;
            List<Object> outArr = new ArrayList<>();
            for(int i=0;i<arr.length();i++){
                outArr.add(returnSchemalessObject(arr.get(i)));
            }
            return outArr;
        }
        else if(obj.equals(JSONObject.NULL)){
            return null;
        }
        // else if(){} // any other stuff here
        else{ // String, Integer, Long, Double, Number.. etc
            return obj;
        }
    }

    static Object returnSchemaObject(Object obj){
        // this is not ready
        return "WITH SCHEMA: NOT READY YET";
    }

}
