package net.coru.kloadgen.input;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import net.coru.kloadgen.model.FieldValueMapping;
import org.apache.avro.Schema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.commons.lang3.tuple.Pair;

public interface SchemaTool {

  Pair<SchemaMetadata, Schema> getSchemaBySubject(String avroSubjectName, List<FieldValueMapping> fieldExprMappings)
      throws IOException, RestClientException;
}
