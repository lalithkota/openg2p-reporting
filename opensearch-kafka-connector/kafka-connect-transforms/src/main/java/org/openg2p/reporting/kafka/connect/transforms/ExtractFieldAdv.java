package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;
import org.apache.kafka.connect.transforms.field.SingleFieldPath;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.openg2p.reporting.kafka.connect.transforms.util.StructMerger;

public abstract class ExtractFieldAdv<R extends ConnectRecord<R>> extends ExtractField<R>{

    public static final String FIELD_CONFIG = "field";
    public static final String ARRAY_MERGE_STRATEGY_CONFIG = "array.merge.strategy";
    public static final String MAP_MERGE_STRATEGY_CONFIG = "map.merge.strategy";

    public static final String PURPOSE = "field extraction";

    public static final char FIELD_SPLITTER = ',';
    public static final String RENAME_FIELD_OPERATOR = "->";

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            FIELD_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.MEDIUM,
            "Field name to extract."
        ).define(
            ARRAY_MERGE_STRATEGY_CONFIG,
            ConfigDef.Type.STRING,
            "concat",
            ConfigDef.Importance.HIGH,
            "Array merge strategy: Available configs: \"concat\", \"replace\"."
        ).define(
            MAP_MERGE_STRATEGY_CONFIG,
            ConfigDef.Type.STRING,
            "deep",
            ConfigDef.Importance.HIGH,
            "Array merge strategy: Available configs: \"deep\", \"replace\"."
        );

    private List<Map.Entry<String, String>> outputPaths;
    private List<Map.Entry<SingleFieldPath, String>> fieldPaths;
    private StructMerger.MapMergeStrategy mapMergeStrategy;
    private StructMerger.ArrayMergeStrategy arrMergeStrategy;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        outputPaths = this.getOutputPaths(config.getString(FIELD_CONFIG));
        mapMergeStrategy = StructMerger.MapMergeStrategy.valueOf(config.getString(MAP_MERGE_STRATEGY_CONFIG));
        arrMergeStrategy = StructMerger.ArrayMergeStrategy.valueOf(config.getString(ARRAY_MERGE_STRATEGY_CONFIG));

        fieldPaths = new ArrayList<>();
        for (Map.Entry<String, String> origPath: outputPaths){
            fieldPaths.add(
                new AbstractMap.SimpleEntry<>(
                    // FieldSyntaxVersion.V2 is mandatory here.
                    new SingleFieldPath(origPath.getValue(), FieldSyntaxVersion.V2),
                    origPath.getKey()
                )
            );
        }
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    public R applySchemaless(R record) {
        Map<String, Object> value = Requirements.requireMapOrNull(operatingValue(record), PURPOSE);
        Object res = null;
        for (Map.Entry<SingleFieldPath, String> fieldPath: fieldPaths){
            Object toBeMerged = getMapOrObject(value, fieldPath.getKey(), fieldPath.getValue());
            if (res == null){
                res = toBeMerged;
            } else if (res instanceof Map && toBeMerged instanceof Map){
                res = StructMerger.mergeMaps((Map<String, Object>)res, (Map<String, Object>)toBeMerged, mapMergeStrategy, arrMergeStrategy);
            } else {
                throw new DataException("ExtractNewFieldAdv: One of the maps trying to be merged is a primitive.");
            }
        }
        return newRecord(record, null, res);
    }

    public R applyWithSchema(R record) {
        Struct value = Requirements.requireStructOrNull(operatingValue(record), PURPOSE);
        Object res = null;
        Schema resSchema = null;

        for (Map.Entry<SingleFieldPath, String> fieldPath: fieldPaths){
            Map.Entry<Object, Schema> toBeMerged = getStructOrObject(value, fieldPath.getKey(), fieldPath.getValue());
            if (res == null){
                res = toBeMerged.getKey();
                resSchema = toBeMerged.getValue();
            } else if (res instanceof Struct && toBeMerged instanceof Struct){
                res = StructMerger.mergeStructs((Struct)res, (Struct)toBeMerged, mapMergeStrategy, arrMergeStrategy);
                resSchema = ((Struct)res).schema();
            } else {
                throw new DataException("ExtractNewFieldAdv: One of the structs trying to be merged is a primitive.");
            }
        }
        return newRecord(record, resSchema, res);
    }

    public Object getMapOrObject(Map<String, Object> value, SingleFieldPath fieldPath, String renameField){
        if (renameField != null && !renameField.isEmpty()){
            Map<String, Object> res = new HashMap<>();
            res.put(renameField, fieldPath.valueFrom(value));
            return res;
        } else {
            return fieldPath.valueFrom(value);
        }
    }

    public Map.Entry<Object, Schema> getStructOrObject(Struct value, SingleFieldPath fieldPath, String renameField){
        Object res = fieldPath.valueFrom(value);
        Schema resSchema = fieldPath.fieldFrom(value.schema()).schema();
        if (renameField != null && !renameField.isEmpty()){
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(((Struct)res).schema());
            builder.field(renameField, ((Struct)res).schema());
            resSchema = builder.build();
            Struct resStruct = new Struct(resSchema);
            resStruct.put(renameField, res);
            return new AbstractMap.SimpleEntry<>(resStruct, resSchema);
        } else {
            return new AbstractMap.SimpleEntry<>(res, resSchema);
        }
    }

    public List<Map.Entry<String, String>> getOutputPaths(String originalPath){
        List<Map.Entry<String, String>> res = new ArrayList<>();
        String[] pathSplit;
        for(String path: originalPath.split(String.valueOf(FIELD_SPLITTER))){
            pathSplit = path.split(RENAME_FIELD_OPERATOR);
            res.add(new AbstractMap.SimpleEntry<>(pathSplit[0], pathSplit.length > 1 ? pathSplit[1] : null));
        }
        return res;
    }

    public static class Key<R extends ConnectRecord<R>> extends ExtractFieldAdv<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractFieldAdv<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
