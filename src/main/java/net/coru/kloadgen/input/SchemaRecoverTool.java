package net.coru.kloadgen.input;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.coru.kloadgen.model.FieldValueMapping;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

public class SchemaRecoverTool implements SchemaTool {

  private final SchemaRegistryClient schemaRegistryClient;

  public SchemaRecoverTool(Map<String, String> originals) {

    schemaRegistryClient = new CachedSchemaRegistryClient(originals.get(SCHEMA_REGISTRY_URL_CONFIG), 1000, originals);
  }

  @Override
  public Pair<SchemaMetadata, Schema> getSchemaBySubject(String avroSubjectName, List<FieldValueMapping> fieldExprMappings) throws IOException, RestClientException {
    SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(avroSubjectName);
    return Pair.of(metadata, schemaRegistryClient.getById(metadata.getId()));
  }
}
