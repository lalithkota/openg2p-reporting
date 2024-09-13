package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;
import org.apache.kafka.connect.transforms.field.SingleFieldPath;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.HashMap;
import java.util.Map;

public abstract class TimestampSelector<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends TimestampSelector<R>{}
    public static class Value<R extends ConnectRecord<R>> extends TimestampSelector<R>{}

    private class Config{
        SingleFieldPath[] tsOrder;
        String outputField;
        String nullValuesBehavior;

        Config(SingleFieldPath[] tso, String outField, String nullValuesBehavior){
            this.tsOrder = tso;
            this.outputField = outField;
            this.nullValuesBehavior = nullValuesBehavior;
        }
    }

    public static final String PURPOSE = "select timestamp in order";
    public static final String TS_ORDER_CONFIG = "ts.order";
    public static final String OUTPUT_FIELD_CONFIG = "output.field";
    public static final String NULL_VALUE_BEHAVIOR_CONFIG = "behavior.on.null.values";

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TS_ORDER_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "The order of the timestamp fields to select from.")
        .define(OUTPUT_FIELD_CONFIG, ConfigDef.Type.STRING, "@ts_generated", ConfigDef.Importance.HIGH, "Name of the resultant/ouptut timestamp field.")
        .define(NULL_VALUE_BEHAVIOR_CONFIG, ConfigDef.Type.STRING, "fail", ConfigDef.Importance.HIGH, "What to do if all the given timestamps are empty? Accepted values: \"fail\", \"ignore\". Default value: \"fail\"");

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema,Schema>(16));

        String tsOrderBulk = absconf.getString(TS_ORDER_CONFIG);
        String outputField = absconf.getString(OUTPUT_FIELD_CONFIG);
        String nullValuesBehavior = absconf.getString(NULL_VALUE_BEHAVIOR_CONFIG);

        if(tsOrderBulk.isEmpty()){
            throw new ConfigException("One of required transform config fields not set. Required field in tranforms: " + TS_ORDER_CONFIG + ". Optional Fields: " + OUTPUT_FIELD_CONFIG);
        }

        if(!("fail".equals(nullValuesBehavior) || "ignore".equals(nullValuesBehavior))){
            throw new ConfigException("Behavior on null values is set incorrectly. Given value: " + nullValuesBehavior);
        }

        String[] tsOrder = tsOrderBulk.replaceAll("\\s+","").split(",");

        if(tsOrder.length == 0){
            throw new ConfigException("Number of fields in timestamp order are zero.");
        }

        SingleFieldPath[] tsFieldPathOrder = new SingleFieldPath[tsOrder.length];
        for (int i=0; i < tsOrder.length; i++){
            tsFieldPathOrder[i] = new SingleFieldPath(tsOrder[i], FieldSyntaxVersion.V2);
        }

        config = new Config(tsFieldPathOrder, outputField, nullValuesBehavior);
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        Object ret=null;
        for(SingleFieldPath fieldPath: config.tsOrder){
            ret = fieldPath.valueFrom(value);
            if(ret!=null && !"".equals(ret)){
                break;
            }
        }
        if(ret==null || "".equals(ret)){
            if ("fail".equals(config.nullValuesBehavior)){
                throw new DataException("None of the fields mentioned in timestamp order have a valid value.");
            } else {
                return record;
            }
        }

        updatedValue.put(config.outputField, ret);

        return newRecord(record, null, updatedValue);
    }

    @Override
    public R applyWithSchema(R record) {
        final Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);

        Object ret = null;
        Schema outFieldSchema = null;
        for(SingleFieldPath fieldPath: config.tsOrder){
            ret = fieldPath.valueFrom(value);
            if(ret!=null && !"".equals(ret)){
                outFieldSchema = fieldPath.fieldFrom(value.schema()).schema();;
                break;
            }
        }
        if(ret==null || "".equals(ret)){
            if ("fail".equals(config.nullValuesBehavior)){
                throw new DataException("None of the fields mentioned in timestamp order have a valid value.");
            } else {
                return record;
            }
        }

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            // Hardcoding to string schema here .. which might not be correct in all cases
            updatedSchema = makeUpdatedSchema(value.schema(), config.outputField, outFieldSchema);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(config.outputField, ret);

        return newRecord(record, updatedSchema, updatedValue);
    }

    public Schema makeUpdatedSchema(Schema schema, String outField, Schema outSchema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(outField, outSchema);

        return builder.build();
    }
}
