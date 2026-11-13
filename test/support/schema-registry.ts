import { readFileSync } from 'node:fs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { DEPLOYAGRAM_NETWORK, networkBootstrapServersOf, SUGGESTIONS_TOPIC } from './kafka.js';

/** The Confluent Schema Registry image, matching the Kafka image's version. */
export const SCHEMA_REGISTRY_IMAGE = 'confluentinc/cp-schema-registry:7.6.1';

const SCHEMA_REGISTRY_PORT = 8081;

/** The Avro schema of the suggestion message, the same file the Java demo uses. */
const SCHEMA_FILE = new URL('../resources/avro/dont_forget_message.avsc', import.meta.url);

/** The subject the value schema is registered under: the topic name with "-value", as Confluent names it. */
export const VALUE_SUBJECT = `${SUGGESTIONS_TOPIC}-value`;

/**
 * Starts a Schema Registry on the Deployagram network, reachable there as `alias`, and talking to
 * the broker that is reachable there as `kafkaAlias`.
 */
export async function startSchemaRegistry(alias: string, kafkaAlias: string): Promise<StartedTestContainer> {
  return new GenericContainer(SCHEMA_REGISTRY_IMAGE)
    .withNetworkMode(DEPLOYAGRAM_NETWORK)
    .withNetworkAliases(alias)
    .withExposedPorts(SCHEMA_REGISTRY_PORT)
    .withEnvironment({
      SCHEMA_REGISTRY_HOST_NAME: alias,
      SCHEMA_REGISTRY_LISTENERS: `http://0.0.0.0:${SCHEMA_REGISTRY_PORT}`,
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: networkBootstrapServersOf(kafkaAlias),
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: '1',
    })
    .withWaitStrategy(Wait.forHttp('/subjects', SCHEMA_REGISTRY_PORT).forStatusCode(200))
    .withStartupTimeout(60_000)
    .start();
}

/** The Schema Registry's URL on the Deployagram network, as the Collector uses it. */
export function networkSchemaRegistryUrlOf(alias: string): string {
  return `http://${alias}:${SCHEMA_REGISTRY_PORT}`;
}

/** The Schema Registry's URL as seen from the test process. */
export function schemaRegistryUrlOf(container: StartedTestContainer): string {
  return `http://${container.getHost()}:${container.getMappedPort(SCHEMA_REGISTRY_PORT)}`;
}

/** The suggestion message's Avro schema, as text. */
export function suggestionSchema(): string {
  return readFileSync(SCHEMA_FILE, 'utf8');
}

/** Registers the suggestion schema for the topic's value, and returns the id to encode messages with. */
export async function registerSuggestionSchema(registry: SchemaRegistry): Promise<number> {
  const { id } = await registry.register(
    { type: SchemaType.AVRO, schema: suggestionSchema() },
    { subject: VALUE_SUBJECT },
  );
  return id;
}
