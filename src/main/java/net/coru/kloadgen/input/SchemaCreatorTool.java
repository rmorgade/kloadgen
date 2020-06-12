package net.coru.kloadgen.input;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static net.coru.kloadgen.util.ProducerKeysHelper.FLAG_YES;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import lombok.SneakyThrows;
import net.coru.kloadgen.exception.KLoadGenException;
import net.coru.kloadgen.model.FieldValueMapping;
import net.coru.kloadgen.serializer.EnrichedRecord;
import net.coru.kloadgen.util.AvroRandomTool;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.threads.JMeterContextService;

public class SchemaCreatorTool implements SchemaTool {

  private SchemaMetadata metadata;

  private final List<FieldValueMapping> fieldExprMappings;

  private final AvroRandomTool randomToolAvro;

  private final Set<Type> typesSet = EnumSet.of(Type.INT, Type.DOUBLE, Type.FLOAT, Type.BOOLEAN, Type.STRING,
      Type.LONG, Type.BYTES, Type.FIXED);

  public SchemaCreatorTool(String avroSchemaName, List<FieldValueMapping> fieldExprMappings) throws KLoadGenException {

    randomToolAvro = new AvroRandomTool();
    this.fieldExprMappings = fieldExprMappings;
  }

  public Schema getSchemaBySubject(String avroSubjectName, List<FieldValueMapping> fieldExprMappings) throws IOException, RestClientException {
    return buildSchemaFromProperties(avroSubjectName, fieldExprMappings);
  }

  private Schema buildSchemaFromProperties(String avroSchemaName, List<FieldValueMapping> fieldExprMappings) {
    RecordBuilder schema = SchemaBuilder.record(avroSchemaName);
    if (!fieldExprMappings.isEmpty()) {
      ArrayDeque<FieldValueMapping> fieldExpMappingsQueue = new ArrayDeque<>(fieldExprMappings);
      FieldValueMapping fieldValueMapping = fieldExpMappingsQueue.element();
      while (!fieldExpMappingsQueue.isEmpty()) {
        if (cleanUpPath(fieldValueMapping, "").contains("[")) {
          String fieldName = getCleanMethodName(fieldValueMapping, "");
          if(fieldValueMapping.getFieldType().contains("map")) {
            schema.fields().name(fieldName).type().map().values(createTypedSchema(fieldValueMapping.getFieldType().replace("-map","")));
          } else {
            schema.fields().name(fieldName).type().array().items(createTypedSchema(fieldValueMapping.getFieldType().replace("-map","")))
          }
          fieldValueMapping = getSafeGetElement(fieldExpMappingsQueue);
        } else if (cleanUpPath(fieldValueMapping, "").contains(".")) {
          String fieldName = getCleanMethodName(fieldValueMapping, "");
          entity.put(fieldName, createObject(entity.getSchema().getField(fieldName).schema(), fieldName, fieldExpMappingsQueue));
          fieldValueMapping = getSafeGetElement(fieldExpMappingsQueue);
        } else {
          entity.put(fieldValueMapping.getFieldName(),
              randomToolAvro.generateRandom(fieldValueMapping.getFieldType(), fieldValueMapping.getValueLength(),
                  fieldValueMapping.getFieldValuesList(),
                  schema.getField(fieldValueMapping.getFieldName())));
          fieldExpMappingsQueue.remove();
          fieldValueMapping = fieldExpMappingsQueue.peek();
        }
      }
    }
    return new EnrichedRecord(metadata, entity);
  }

  private Schema createTypedSchema(String fieldType)
      throws KLoadGenException {
    Type type;
    switch (fieldType) {
      case "int":
        type = Type.INT;
        break;
      case "long":
      case "timestamp":
      case "longTimestamp":
      case "stringTimestamp":
        type = Type.LONG;
        break;
      case "double":
        type = Type.DOUBLE;
        break;
      case "short":
        type = Type.BYTES;
        break;
      case "boolean":
        type = Type.BOOLEAN;
        break;
      default:
        type = Type.STRING;
        break;
    }
    return Schema.create(type);
  }

  private Schema extractRecordSchema(Field field) {
    if (ARRAY == field.schema().getType()) {
      return field.schema().getElementType();
    } else if (MAP == field.schema().getType()) {
      return field.schema().getElementType();
    } else if (UNION == field.schema().getType()) {
      return getRecordUnion(field.schema().getTypes());
    } else if (typesSet.contains(field.schema().getType())){
      return getRecordUnion(field.schema().getTypes());
    }else return null;
  }

  private Schema getRecordUnion(List<Schema> types) {
    Schema isRecord = null;
    for (Schema schema : types) {
      if (RECORD == schema.getType() || ARRAY == schema.getType() || MAP == schema.getType() || typesSet.contains(schema.getType())) {
        isRecord = schema;
      }
    }
    return isRecord;
  }

  private FieldValueMapping getSafeGetElement(ArrayDeque<FieldValueMapping> fieldExpMappingsQueue) {
    return !fieldExpMappingsQueue.isEmpty() ? fieldExpMappingsQueue.element() : null;
  }

  private String cleanUpPath(FieldValueMapping fieldValueMapping, String fieldName) {
    int startPosition = 0;
    String cleanPath;
    if (StringUtils.isNotEmpty(fieldName)) {
      startPosition = fieldValueMapping.getFieldName().indexOf(fieldName) + fieldName.length() + 1;
    }
    cleanPath = fieldValueMapping.getFieldName().substring(startPosition);
    if (cleanPath.matches("^(\\d*]).*$")) {
      cleanPath = cleanPath.substring(cleanPath.indexOf(".") + 1);
    }
    return cleanPath;
  }

  private String getCleanMethodName(FieldValueMapping fieldValueMapping, String fieldName) {
    String pathToClean = cleanUpPath(fieldValueMapping, fieldName);
    int endOfField = pathToClean.contains(".")?
        pathToClean.indexOf(".") : 0;
    return pathToClean.substring(0, endOfField).replaceAll("\\[[0-9]*]", "");
  }

}
