# connect-schemaless-json

Though there does exist a converter in the `connect-json` [library](https://github.com/apache/kafka/tree/trunk/connect/json/src/main/java/org/apache/kafka/connect/json) called "[JsonConverter](https://github.com/apache/kafka/blob/trunk/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java)", there are limitations as to the domain of JSON payloads this converter is compatible with on the Sink Connector side when serializing them into Kafka Connect datatypes; When reading bytestreams from Kafka, the JsonConverter expects its inputs to be a JSON envelope that contains the fields "schema" and "payload", otherwise it'll throw a DataException reporting:

> JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.

(when `schemas.enable` is [true](https://github.com/apache/kafka/blob/1.1/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java#L338))
or

> JSON value converted to Kafka Connect must be in envelope containing schema

(when `schemas.enable` is [false](https://github.com/apache/kafka/blob/1.1/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java#L358))

For example, if your JSON payload looks something on the order of:

> {
        "c1": 10000,
        "c2": "bar",
        "create_ts": 1501834166000,
        "update_ts": 1501834166000
    }

This will not be compatible for Sink Connectors that require the schema for data ingest when mapping from Kafka Connect datatypes to, for example, JDBC datatypes. Rather, that data is expected to be structured like so:

> {
    "schema": {
        "type": "struct",
        "fields": [{
            "type": "int32",
            "optional": true,
            "field": "c1"
        }, {
            "type": "string",
            "optional": true,
            "field": "c2"
        }, {
            "type": "int64",
            "optional": false,
            "name": "org.apache.kafka.connect.data.Timestamp",
            "version": 1,
            "field": "create_ts"
        }, {
            "type": "int64",
            "optional": false,
            "name": "org.apache.kafka.connect.data.Timestamp",
            "version": 1,
            "field": "update_ts"
        }],
        "optional": false,
        "name": "foobar"
    },
    "payload": {
        "c1": 10000,
        "c2": "bar",
        "create_ts": 1501834166000,
        "update_ts": 1501834166000
    }
}

The "schema" is a necessary component in order to dictate to the JsonConverter how to map the payload's JSON datatypes to Kafka Connect datatypes on the consumer side. This library introduces an enhanced version of the JsonConverter called SchemalessJsonConverter. It's modeled after the original (subclassing it), but overrides the toConnectData() method so that it is capable of taking in any JSON and converting it into Kafka Connect datatypes (SchemaAndValue) by inferring every single object within the JSON message based on its [JSON datatype](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/JsonNode.html#getNodeType()).

To use it, simply place the jar file into your `plugin.path` directory (released in 0.11.0.0 of `connect-runtime`) or include this library in your classpath.

Once loaded into your application, the connector properties for your Sink Connectors need this configuration:
`"value.converter": "org.apache.kafka.connect.json.SchemalessJsonConverter"`
