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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.EnumMap;
import java.util.TimeZone;


public abstract class TimestampConverterAdv<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends TimestampConverterAdv<R>{}
    public static class Value<R extends ConnectRecord<R>> extends TimestampConverterAdv<R>{}

    public static class Config {
        public enum InputType {
            milli_sec,
            micro_sec,
            days_epoch
        }

        public enum OutputType {
            string
        }

        String field;
        InputType inType;
        OutputType outType;
        SimpleDateFormat format;

        EnumMap<OutputType, Schema.Type> OUTPUT_TYPE_MAP;

        Config(String field, InputType inType, OutputType outType, SimpleDateFormat format) {
            this.field = field;
            this.inType = inType;
            this.outType = outType;
            this.format = format;

            Map<OutputType, Schema.Type> outTypeSchemaMap = new HashMap<>();
            outTypeSchemaMap.put(OutputType.string, Schema.Type.STRING);

            OUTPUT_TYPE_MAP = new EnumMap<>(outTypeSchemaMap);
        }

        public Schema.Type getOutputSchemaType(){
            return OUTPUT_TYPE_MAP.get(outType);
        }
    }

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static String PURPOSE = "converting timestamp formats";

    public static String FIELD_CONFIG = "field";
    public static String INPUT_TYPE_CONFIG = "input.type";
    public static String OUTPUT_TYPE_CONFIG = "output.type";
    public static String OUTPUT_FORMAT_CONFIG = "output.format";
    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "The field containing the timestamp, or empty if the entire value is a timestamp")
        .define(INPUT_TYPE_CONFIG, ConfigDef.Type.STRING, "milli_sec", ConfigDef.Importance.HIGH, "The field containing the type of the input. Supported values: milli_sec(default)/micro_sec/days_epoch")
        .define(OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, "string", ConfigDef.Importance.HIGH, "Output Type. Supported values: string")
        .define(OUTPUT_FORMAT_CONFIG, ConfigDef.Type.STRING, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", ConfigDef.Importance.HIGH, "Format of timestamp output");

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        String field = absconf.getString(FIELD_CONFIG);
        String inType = absconf.getString(INPUT_TYPE_CONFIG);
        String outType = absconf.getString(OUTPUT_TYPE_CONFIG);
        String outFormatPattern = absconf.getString(OUTPUT_FORMAT_CONFIG);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema,Schema>(16));

        Config.InputType inTypeEnum = null;
        try{
            inTypeEnum = Config.InputType.valueOf(inType);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("TimestampConverter: invalid " + INPUT_TYPE_CONFIG);
        }

        Config.OutputType outTypeEnum = null;
        try{
            outTypeEnum = Config.OutputType.valueOf(outType);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("TimestampConverter: invalid " + OUTPUT_TYPE_CONFIG);
        }

        SimpleDateFormat format = null;
        try {
            format = new SimpleDateFormat(outFormatPattern);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
        } catch (IllegalArgumentException e) {
            throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: " + outFormatPattern, e);
        }

        config = new Config(field,inTypeEnum,outTypeEnum,format);
    }

    @Override
    public R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (config.field.isEmpty()) {
            Object value = operatingValue(record);
            Object defaultValue = convertTimestamp(schema.defaultValue(), config.inType, config.format);
            Schema updatedSchema = SchemaBuilder.type(config.getOutputSchemaType()).optional().defaultValue(defaultValue).build();
            return newRecord(record, updatedSchema, convertTimestamp(value, config.inType, config.format));
        } else {
            final Struct value = Requirements.requireStructOrNull(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (field.name().equals(config.field)) {
                        Object defaultValue = convertTimestamp(field.schema().defaultValue(), config.inType, config.format);
                        builder.field(field.name(), SchemaBuilder.type(config.getOutputSchemaType()).optional().defaultValue(defaultValue).build());
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(value.get(field), config.inType, config.format);
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    @Override
    public R applySchemaless(R record) {
        Object rawValue = operatingValue(record);
        if (rawValue == null || config.field.isEmpty()) {
            return newRecord(record, null, convertTimestamp(rawValue, config.inType, config.format));
        } else {
            final Map<String, Object> value = Requirements.requireMap(rawValue,PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(config.field, convertTimestamp(value.get(config.field), config.inType, config.format));
            return newRecord(record, null, updatedValue);
        }
    }

    private String convertTimestamp(Object timestamp, Config.InputType inType, SimpleDateFormat format) {
        if (timestamp == null) {
            return null;
        }

        String output;
        long tsLong = ((Number)timestamp).longValue();;

        if(inType == Config.InputType.milli_sec){
            output = format.format(new Date(tsLong));
        } else if(inType == Config.InputType.micro_sec){
            output = format.format(new Date(tsLong/1000));
            // // the following is if there are more micro digits... right now ignoring
            // if((tsLong%1000) != 0) {
            //     output = output.substring(0,s.length()-1);
            //     output += (tsLong%1000);
            //     output += "Z";
            // }
        } else if(inType == Config.InputType.days_epoch){
            output = format.format(new Date(tsLong*24*60*60*1000));
        } else{
            // it should never come here
            return "";
        }

        return output;
    }
}
