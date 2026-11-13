import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { Kafka, logLevel, type EachMessageHandler } from 'kafkajs';
import { sendEmail } from './emailer-client.js';
import { emailFor, type SuggestionMessage } from './suggestion-email.js';

// Deployagram's qualified name for this cross-org topic (ORG_ctm_01m1j08sx96792d2tysabvyadp:SuppliedSuggestionsTopic),
// with "__COLON__" standing in for ":", which is not legal in a Kafka topic name.
export const SUGGESTIONS_TOPIC = 'ORG_ctm_01m1j08sx96792d2tysabvyadp__COLON__SuppliedSuggestionsTopic';
export const CONSUMER_GROUP = 'boot-dont-forget';

export interface AppConfig {
  kafkaBrokers: string[];
  /** The Confluent Schema Registry that holds the Avro schema of the messages on the topic. */
  schemaRegistryUrl: string;
  emailerBaseUrl: string;
  /**
   * Wraps the message handler, for example to add behaviour around each read. The application
   * itself has no instrumentation; the test supplies this.
   */
  wrapEachMessage?: (handler: EachMessageHandler) => EachMessageHandler;
}

export interface RunningApplication {
  stop(): Promise<void>;
}

/**
 * Reads Avro suggestions from the suggestions topic and emails each one to the Emailer.
 * The consumer starts from the earliest message, and resolves once it has joined its group, so no
 * message is missed.
 */
export async function startApplication(config: AppConfig): Promise<RunningApplication> {
  const kafka = new Kafka({
    clientId: 'deployagram-dont-forget',
    brokers: config.kafkaBrokers,
    logLevel: logLevel.WARN,
  });
  const registry = new SchemaRegistry({ host: config.schemaRegistryUrl });
  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

  const joined = new Promise<void>((resolve) => {
    consumer.on(consumer.events.GROUP_JOIN, () => resolve());
  });

  await consumer.connect();
  await consumer.subscribe({ topic: SUGGESTIONS_TOPIC, fromBeginning: true });
  const handleMessage: EachMessageHandler = async ({ message }) => {
    if (!message.value) {
      return;
    }
    // The Avro schema id is in the message, so the registry supplies the writer's schema.
    const suggestion = (await registry.decode(message.value)) as SuggestionMessage;
    await sendEmail(config.emailerBaseUrl, emailFor(suggestion));
  };
  await consumer.run({
    eachMessage: config.wrapEachMessage ? config.wrapEachMessage(handleMessage) : handleMessage,
  });
  await joined;

  return {
    async stop() {
      await consumer.disconnect();
    },
  };
}
