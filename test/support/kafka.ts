import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { StartedNetwork } from 'testcontainers';
import { Kafka } from 'kafkajs';

/**
 * The Kafka image. apache/kafka-native does not work with the testcontainers Kafka module, which
 * starts the broker with /etc/confluent/docker/run, a script only Confluent's images provide.
 */
export const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.6.1';
// Deployagram's qualified name for this cross-org topic, the same name the application reads.
// "__COLON__" stands in for ":", which is not legal in a Kafka topic name.
export const SUGGESTIONS_TOPIC = 'ORG_ctm_01m1j08sx96792d2tysabvyadp__COLON__SuppliedSuggestionsTopic';

// The port testcontainers' Kafka image listens on inside the container.
const KAFKA_INTERNAL_PORT = 9093;
// The broker's listener for other containers. The module advertises it under the network alias.
const KAFKA_NETWORK_PORT = 9092;

/**
 * Starts a broker on the network, reachable there as `alias`. Other containers on the network can reach it by that name.
 */
export async function startKafka(network: StartedNetwork, alias: string): Promise<StartedKafkaContainer> {
  return new KafkaContainer(KAFKA_IMAGE).withKraft().withNetwork(network).withNetworkAliases(alias).start();
}

/** The broker's address on the network, as other containers on it use it. */
export function networkBootstrapServersOf(alias: string): string {
  return `PLAINTEXT://${alias}:${KAFKA_NETWORK_PORT}`;
}

/** Bootstrap servers for a started Kafka container, as seen from the test process. */
export function brokersOf(container: StartedKafkaContainer): string[] {
  return [`${container.getHost()}:${container.getMappedPort(KAFKA_INTERNAL_PORT)}`];
}

export async function createTopic(brokers: string[], topic: string): Promise<void> {
  const admin = new Kafka({ clientId: 'test-admin', brokers }).admin();
  await admin.connect();
  try {
    await admin.createTopics({ topics: [{ topic }], waitForLeaders: true });
  } finally {
    await admin.disconnect();
  }
}

/** Sends one message, with the given bytes as its value, to the topic. */
export async function produce(brokers: string[], topic: string, value: Buffer): Promise<void> {
  const producer = new Kafka({ clientId: 'test-producer', brokers }).producer();
  await producer.connect();
  try {
    await producer.send({ topic, messages: [{ value }] });
  } finally {
    await producer.disconnect();
  }
}
