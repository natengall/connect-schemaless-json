package org.apache.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of Converter that uses JSON to store schemas and objects.
 */
public class SchemalessJsonConverter extends JsonConverter {
    private static final String SCHEMAS_REMAP_CONFIG = "schemas.remap";

    private HashMap<String, String> schemaRemap = new HashMap<String, String>();

    private JsonDeserializer deserializer = new JsonDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);

        String schemasRemap = (String) configs.get(SCHEMAS_REMAP_CONFIG);
        if (StringUtils.isNotEmpty(schemasRemap)) {
            for (String remapping : schemasRemap.split(",")) {
                String[] components = remapping.split(":");
                if (components.length == 2) {
                    schemaRemap.put(components[0].trim(), components[1].trim());
                }
            }
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (jsonValue == null)
            return SchemaAndValue.NULL;

        LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
        SchemaBuilder builder = SchemaBuilder.struct();
        flattenAndMap(null, jsonValue, map, builder);
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            struct.put(entry.getKey(), entry.getValue());
        }

        return new SchemaAndValue(schema, struct);
    }

    private void flattenAndMap(String key, JsonNode jsonValue, LinkedHashMap<String, Object> map, SchemaBuilder builder) {
        if (schemaRemap.containsKey(key)) {
            key = schemaRemap.get(key);
        }

        switch (jsonValue.getNodeType()) {
            case NULL:
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
                map.put(key, null);
                break;
            case BOOLEAN:
                builder.field(key, Schema.BOOLEAN_SCHEMA);
                map.put(key, jsonValue.booleanValue());
                break;
            case NUMBER:
                if (jsonValue.isIntegralNumber()) {
                    builder.field(key, Schema.INT64_SCHEMA);
                    map.put(key, jsonValue.longValue());
                }
                else {
                    builder.field(key, Schema.FLOAT64_SCHEMA);
                    map.put(key, jsonValue.doubleValue());
                }
                break;
            case OBJECT:
                Iterator<Map.Entry<String, JsonNode>> it = jsonValue.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    flattenAndMap(entry.getKey(), entry.getValue(), map, builder);
                }
                break;
            case STRING:
                builder.field(key, Schema.STRING_SCHEMA);
                map.put(key, jsonValue.textValue());
                break;
            default:
                break;
        }
    }
}
