package org.openg2p.reporting.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsStore;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import org.json.JSONObject;
import org.json.JSONException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class DynamicNewField<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    public static class Key<R extends ConnectRecord<R>> extends DynamicNewField<R>{}
    public static class Value<R extends ConnectRecord<R>> extends DynamicNewField<R>{}

    public abstract class Config{
        String type;
        String[] inputFields;
        String[] inputDefaultValues;
        String[] outputFields;
        Schema outputSchema;
        Config(String type, String[] inputFields, String[] inputDefaultValues, String[] outputFields, Schema outputSchema){
            this.type = type;
            this.inputFields = inputFields;
            this.inputDefaultValues = inputDefaultValues;
            this.outputFields = outputFields;
            this.outputSchema = outputSchema;
        }
        List<Object> make(Object input){
            return null;
        }
        List<List<Object>> makeList(Object input){
            return null;
        }
        void close(){
        }
    }

    public class ESQueryConfig extends Config{
        String esUrl;
        String esIndex;
        String[] esInputFields;
        String[] esOutputFields;
        String esInputQueryAddKeyword;

        // RestHighLevelClient esClient;
        CloseableHttpClient hClient;
        HttpGet hGet;

        ESQueryConfig(
            String type,
            String[] inputFields,
            String[] outputFields,
            String[] inputDefaultValues,
            String esUrl,
            String esIndex,
            String[] esInputFields,
            String[] esOutputFields,
            String esInputQueryAddKeyword,
            String esSecurity,
            String esUsername,
            String esPassword
        ) {
            super(type,inputFields,inputDefaultValues,outputFields,Schema.OPTIONAL_STRING_SCHEMA);

            this.esUrl=esUrl;
            this.esIndex=esIndex;
            this.esInputFields=esInputFields;
            this.esOutputFields=esOutputFields;
            this.esInputQueryAddKeyword=esInputQueryAddKeyword;

            // esClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(this.esUrl)));
            HttpClientBuilder hClientBuilder = HttpClients.custom();
            if(esSecurity!=null && !esSecurity.isEmpty() && "true".equals(esSecurity)) {
                CredentialsStore esCredStore = new BasicCredentialsProvider();
                esCredStore.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(esUsername, esPassword.toCharArray()));
                hClientBuilder.setDefaultCredentialsProvider(esCredStore);
            }
            hClient = hClientBuilder.build();
            hGet = new HttpGet(this.esUrl+"/"+this.esIndex+"/_search");
            hGet.setHeader("Content-type", "application/json");
        }

        List<Object> makeQuery(List<Object> inputValues){
            if(inputValues.size()!=inputFields.length){
                return Collections.nCopies(esOutputFields.length, "Cant get all values for the mentioned " + INPUT_FIELDS_CONFIG + ". Given " + INPUT_FIELDS_CONFIG + " : " + Arrays.toString(inputFields)+ " " + inputValues);
            }
            else if(inputValues.size()==0){
                return Collections.nCopies(esOutputFields.length, null);
            }

            String requestJson = "{\"query\": { \"bool\": { \"must\": [";

            for(int i=0; i<inputFields.length; i++){
                if(i!=0)requestJson+=",";
                requestJson += "{\"term\": {";

                Object value = inputValues.get(i);

                if(!"true".equals(this.esInputQueryAddKeyword)){
                    requestJson += "\"" + esInputFields[i] + "\": ";
                } else {
                    requestJson += "\"" + esInputFields[i] + ".keyword\": ";
                }
                requestJson += (value instanceof Number || value instanceof Boolean) ? value : "\"" + value + "\"";
                requestJson += "}}";
            }
            requestJson += "]}}}";

            hGet.setEntity(new StringEntity(requestJson));

            JSONObject responseJson;
            JSONObject responseSource = null;
            List<Object> outputValues = new ArrayList<>();

            final int MAX_RETRIES = 5;
            for(int i=1; i <= MAX_RETRIES; i++){
                try(CloseableHttpResponse hResponse = hClient.execute(hGet)){
                    HttpEntity entity = hResponse.getEntity();
                    String jsonString = EntityUtils.toString(entity);
                    responseJson = new JSONObject(jsonString);
                }
                catch(Exception e){
                    if(i==MAX_RETRIES) return Collections.nCopies(esOutputFields.length, "Error occured while making the query : " + e.getMessage());
                    else continue;
                }

                try{
                    responseSource = responseJson.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source");
                    break;
                } catch(JSONException je){
                    // do nothing
                }
            }
            if(responseSource == null) return Collections.nCopies(esOutputFields.length, "Error: No hits found");
            for(String esOutputField: esOutputFields){
                if(responseSource.has(esOutputField) && !responseSource.isNull(esOutputField)){
                    outputValues.add(responseSource.get(esOutputField));
                } else {
                    outputValues.add(null);
                }
            }
            return outputValues;
        }

        List<List<Object>> makeQueryForList(List<Object> inputValues){
            // Denormalizing List of Lists here
            // [                        [
            //   "abc",                   ["abc","123","xyz","hello"],
            //   "123",            ==>    ["abc","123","pqr","world"]
            //   ["xyz","pqr"],         ]
            //   ["hello","world"]
            // ]
            int arraySize = -1;
            for(Object v : inputValues){
                if(v instanceof List){
                    if(arraySize == -1) arraySize = ((List<Object>)v).size();
                    else if(arraySize != ((List<Object>)v).size()) throw new DataException("Irregular Array List Sizes");
                }
            }
            List<Object> input = new ArrayList<>();
            List<List<Object>> output = new ArrayList<>();

            for(int j = 0; j < arraySize; j++){
                List<Object> list = new ArrayList<Object>();
                for(int i = 0; i < inputValues.size(); i++){

                    if(inputValues.get(i) instanceof List){
                        list.add(((List<Object>)inputValues.get(i)).get(j));
                    }
                    else{
                        list.add(inputValues.get(i));
                    }
                }
                input.add(list);

            }

            for(Object v : input){
                output.add(make(v));
            }

            return output;
        }

        @Override
        List<Object> make(Object input){
            return this.makeQuery((List<Object>)input);
        }

        @Override
        List<List<Object>> makeList(Object input){
            return this.makeQueryForList((List<Object>)input);
        }

        @Override
        void close(){
            try{hClient.close();}catch(Exception e){}
        }

    }

    public static final String PURPOSE = "dynamic field insertion";
    public static final String TYPE_CONFIG = "query.type";

    // Base Config
    public static final String INPUT_FIELDS_CONFIG = "input.fields";
    public static final String OUTPUT_FIELDS_CONFIG = "output.fields";
    public static final String DEFAULT_VALUE_CONFIG = "input.default.values";

    // Elasticsearch Specific Config
    public static final String ES_URL_CONFIG = "es.url";
    public static final String ES_INDEX_CONFIG = "es.index";
    public static final String ES_INPUT_FIELDS_CONFIG = "es.input.fields";
    public static final String ES_OUTPUT_FIELDS_CONFIG = "es.output.fields";
    public static final String ES_INPUT_QUERY_ADD_KEYWORD = "es.input.query.add.keyword";
    public static final String ES_SECURITY_ENABLED_CONFIG = "es.security.enabled";
    public static final String ES_USERNAME_CONFIG = "es.username";
    public static final String ES_PASSWORD_CONFIG = "es.password";

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TYPE_CONFIG, ConfigDef.Type.STRING, "es", ConfigDef.Importance.HIGH, "This is the type of query made. For now this field is ignored and defaulted to es")
        .define(INPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name of the field in the current index")
        .define(OUTPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Names to give to the new fields")
        .define(DEFAULT_VALUE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Default vlaues for input fields")

        .define(ES_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Installed Elasticsearch URL")
        .define(ES_INDEX_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name of the index in ES to search")
        .define(ES_INPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "ES documents with given input field will be searched for. This field tells the key name")
        .define(ES_OUTPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "If a successful match is made with the above input field+value, the values of this output fields from the same document will be returned")
        .define(ES_INPUT_QUERY_ADD_KEYWORD, ConfigDef.Type.STRING, "false", ConfigDef.Importance.HIGH, "Should add the .keyword suffix while querying ES?")

        .define(ES_SECURITY_ENABLED_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Is Elasticsearch security enabled?")
        .define(ES_USERNAME_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Elasticsearch Username")
        .define(ES_PASSWORD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Elasticsearch Password");


    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema,Schema>(16));

        String type = absconf.getString(TYPE_CONFIG);

        String inputFieldBulk = absconf.getString(INPUT_FIELDS_CONFIG);
        String outputFieldBulk = absconf.getString(OUTPUT_FIELDS_CONFIG);
        String inputDefaultValuesBulk = absconf.getString(DEFAULT_VALUE_CONFIG);

        if (type.isEmpty() || inputFieldBulk.isEmpty() || outputFieldBulk.isEmpty()) {
            throw new ConfigException("One of required transform base config fields not set. Required base fields in tranform: " + TYPE_CONFIG + " ," + INPUT_FIELDS_CONFIG + " ," + OUTPUT_FIELDS_CONFIG);
        }

        String[] inputFields = inputFieldBulk.replaceAll("\\s+","").split(",");
        String[] outputFields = outputFieldBulk.replaceAll("\\s+","").split(",");
        String[] inputDefaultValuesInput = inputDefaultValuesBulk.replaceAll("\\s+","").split(",");
        String[] inputDefaultValues = new String[inputFields.length];
        System.arraycopy(inputDefaultValuesInput, 0, inputDefaultValues, 0, inputDefaultValuesInput.length);

        if(type.equals("es")){
            String esUrl = absconf.getString(ES_URL_CONFIG);
            String esIndex = absconf.getString(ES_INDEX_CONFIG);
            String esSecurity = absconf.getString(ES_SECURITY_ENABLED_CONFIG);
            String esUsername = absconf.getString(ES_USERNAME_CONFIG);
            String esPassword = absconf.getString(ES_PASSWORD_CONFIG);
            String esInputFieldBulk = absconf.getString(ES_INPUT_FIELDS_CONFIG);
            String esOutputFieldBulk = absconf.getString(ES_OUTPUT_FIELDS_CONFIG);
            String esInputQueryAddKeyword = absconf.getString(ES_INPUT_QUERY_ADD_KEYWORD);

            if(esUrl.isEmpty() || esIndex.isEmpty() || esInputFieldBulk.isEmpty() || esOutputFieldBulk.isEmpty()){
                throw new ConfigException("One of required transform Elasticsearch config fields not set. Required Elasticsearch fields in tranform: " + ES_URL_CONFIG + " ," + ES_INDEX_CONFIG + " ," + ES_INPUT_FIELDS_CONFIG + " ," + ES_OUTPUT_FIELDS_CONFIG);
            }

            String[] esInputFields = esInputFieldBulk.replaceAll("\\s+","").split(",");
            String[] esOutputFields = esOutputFieldBulk.replaceAll("\\s+","").split(",");

            if(inputFields.length != esInputFields.length){
                throw new ConfigException("No of " + INPUT_FIELDS_CONFIG + " and no of " + ES_INPUT_FIELDS_CONFIG + " doesnt match. Given " + INPUT_FIELDS_CONFIG + ": " + inputFieldBulk + ". Given " + ES_INPUT_FIELDS_CONFIG + ": " + esInputFieldBulk);
            }

            try{
                config = new ESQueryConfig(
                    type,
                    inputFields,
                    outputFields,
                    inputDefaultValues,
                    esUrl,
                    esIndex,
                    esInputFields,
                    esOutputFields,
                    esInputQueryAddKeyword,
                    esSecurity,
                    esUsername,
                    esPassword
                );
            }
            catch(Exception e){
                throw new ConfigException("Can't connect to ElasticSearch. Given url : " + esUrl + " Error: " + e.getMessage());
            }
        }
        else{
            throw new ConfigException("Unknown Type : " + type + ". Available types: \"es\"" );
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
        if(config != null) config.close();
    }

    @Override
    public R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        List<Object> valueList = new ArrayList<Object>();
        boolean dealingWithList = false;
        for(int i = 0; i < config.inputFields.length; i++){
            Object v = Requirements.getNestedField(value, config.inputFields[i]);
            if(v != null && !(v instanceof String && ((String)v).isEmpty())){
                valueList.add(v);
                if(v instanceof List<?>) dealingWithList = true;
            } else {
                valueList.add(config.inputDefaultValues[i] != null && !config.inputDefaultValues[i].isEmpty() ? config.inputDefaultValues[i] : null);
            }
        }

        Object output = dealingWithList ? config.makeList(valueList) : config.make(valueList);
        List<Object> outputList = (List<Object>)output;
        for(int i=0; i<config.outputFields.length; i++) {
            updatedValue.put(config.outputFields[i], outputList.get(i));
        }

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
            updatedValue.put(field.name(), value.get(field));
        }

        List<Object> valueList = new ArrayList<Object>();
        boolean dealingWithList = false;
        for(int i = 0; i < config.inputFields.length; i++){
            Object v = Requirements.getNestedField(value, config.inputFields[i])[0];
            if(v != null && !(v instanceof String && ((String)v).isEmpty())){
                valueList.add(v);
                if(v instanceof List) dealingWithList = true;
            } else {
                valueList.add(config.inputDefaultValues[i] != null && !config.inputDefaultValues[i].isEmpty() ? config.inputDefaultValues[i] : null);
            }
        }

        Object output = dealingWithList ? config.makeList(valueList) : config.make(valueList);
        List<Object> outputList = (List<Object>)output;
        for (int i=0; i<config.outputFields.length; i++){
            updatedValue.put(config.outputFields[i], outputList.get(i));
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    public Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        for (String outputField: config.outputFields){
            builder.field(outputField, config.outputSchema);
        }

        return builder.build();
    }
}
