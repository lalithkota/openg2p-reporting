package org.openg2p.reporting.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class BaseTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    public static ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> configs) {
        // To be overridden
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close(){
        // To be overridden
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    public R applySchemaless(R record){
        // To be overridden
        return record;
    }

    public R applyWithSchema(R record){
        // To be overridden
        return record;
    }

    public Schema operatingSchema(R record){
        if (this.getClass().getName().endsWith("Key")) {
            return record.keySchema();
        } else if (this.getClass().getName().endsWith("Value")) {
            return record.valueSchema();
        } else {
            return record.valueSchema();
        }
    }

    public Object operatingValue(R record){
        if (this.getClass().getName().endsWith("Key")) {
            return record.key();
        } else if (this.getClass().getName().endsWith("Value")) {
            return record.value();
        } else {
            return record.value();
        }
    }

    public R newRecord(R record, Schema updatedSchema, Object updatedValue){
        if (this.getClass().getName().endsWith("Key")) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        } else if (this.getClass().getName().endsWith("Value")) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        } else {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
