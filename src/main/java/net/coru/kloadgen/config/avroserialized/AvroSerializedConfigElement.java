package net.coru.kloadgen.config.avroserialized;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.coru.kloadgen.loadgen.BaseLoadGenerator;
import net.coru.kloadgen.loadgen.impl.AvroLoadGenerator;
import net.coru.kloadgen.model.FieldValueMapping;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.event.LoopIterationListener;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import java.util.List;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.BEARER_AUTH_TOKEN_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG;
import static net.coru.kloadgen.util.ProducerKeysHelper.FLAG_YES;
import static net.coru.kloadgen.util.ProducerKeysHelper.SAMPLE_ENTITY;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY;
import static net.coru.kloadgen.util.SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL;

@Getter
@Setter
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class AvroSerializedConfigElement extends ConfigTestElement implements TestBean, LoopIterationListener {

  private String avroSubject;

  private List<FieldValueMapping> schemaProperties;

  private BaseLoadGenerator generator;

  @Override
  public void iterationStart(LoopIterationEvent loopIterationEvent) {

    try {
      if (generator ==  null) {
        HashMap<String, String> originals = new HashMap<>();
        Properties ctxProperties = JMeterContextService.getContext().getProperties();
        if (Objects.nonNull(ctxProperties.getProperty(SCHEMA_REGISTRY_URL))) {
          originals.put(SCHEMA_REGISTRY_URL_CONFIG, ctxProperties.getProperty(SCHEMA_REGISTRY_URL));

          if (FLAG_YES.equals(ctxProperties.getProperty(SCHEMA_REGISTRY_AUTH_FLAG))) {
            if (SCHEMA_REGISTRY_AUTH_BASIC_TYPE
                .equals(ctxProperties.getProperty(SCHEMA_REGISTRY_AUTH_KEY))) {
              originals.put(BASIC_AUTH_CREDENTIALS_SOURCE,
                  ctxProperties.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
              originals.put(USER_INFO_CONFIG, ctxProperties.getProperty(USER_INFO_CONFIG));
            } else {
              originals.put(BEARER_AUTH_CREDENTIALS_SOURCE,
                  ctxProperties.getProperty(BEARER_AUTH_CREDENTIALS_SOURCE));
              originals.put(BEARER_AUTH_TOKEN_CONFIG, ctxProperties.getProperty(BEARER_AUTH_TOKEN_CONFIG));
            }
          }
          generator = new AvroLoadGenerator(avroSubject, schemaProperties);
      }

      JMeterVariables variables = JMeterContextService.getContext().getVariables();
      variables.putObject(SAMPLE_ENTITY, generator.nextMessage());
    } catch (Exception e) {
      log.error("Failed to create AvroLoadGenerator instance", e);
    }
  }

}
