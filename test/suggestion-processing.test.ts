import { http, passthrough } from 'msw';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import { Network, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { startApplication, type RunningApplication } from '../src/app.js';
import { brokersOf, createTopic, produce, SUGGESTIONS_TOPIC, startKafka } from './support/kafka.js';
import { EMAIL_PATH, EMAILER_BASE_URL, EmailerDouble } from './support/emailer-double.js';
import { registerSuggestionSchema, schemaRegistryUrlOf, startSchemaRegistry } from './support/schema-registry.js';

/** The suggestion written to the topic. */
const SUGGESTION_MESSAGE = {
  email: 'vitest@nodejs.com',
  suggestion: {
    name: 'Refactoring',
    edition: '1st',
    format: 'paperback',
    authors: ['Fowler Martin', 'Beck Kent'],
  },
};

/** The email the application should send for that suggestion. Authors are reduced to surnames. */
const EXPECTED_EMAIL = {
  email: 'vitest@nodejs.com',
  subject: "Don't forget to check this out!",
  body: "We think you've recently considered Refactoring (Fowler, Beck). The 1st edition is available in paperback!",
};

describe('SuggestionProcessingTest', () => {
  let network: StartedNetwork;
  let kafka: StartedKafkaContainer;
  let schemaRegistry: StartedTestContainer;
  let registry: SchemaRegistry;
  let schemaId: number;
  let emailer: EmailerDouble;
  let application: RunningApplication;
  let brokers: string[];

  beforeAll(async () => {
    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `dontforget-kafka-${suffix}`;
    const registryAlias = `dontforget-schema-registry-${suffix}`;

    network = await new Network().start();
    kafka = await startKafka(network, kafkaAlias);
    brokers = brokersOf(kafka);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(network, registryAlias, kafkaAlias);
    const schemaRegistryUrl = schemaRegistryUrlOf(schemaRegistry);
    registry = new SchemaRegistry({ host: schemaRegistryUrl });
    schemaId = await registerSuggestionSchema(registry);

    emailer = new EmailerDouble();
    // The Schema Registry client makes HTTP calls. Let them reach its server, rather than treating them as unhandled.
    emailer.server.use(http.all(`${schemaRegistryUrl}/*`, () => passthrough()));
    await emailer.start();

    application = await startApplication({
      kafkaBrokers: brokers,
      schemaRegistryUrl,
      emailerBaseUrl: EMAILER_BASE_URL,
    });
  }, 180_000);

  afterAll(async () => {
    await application?.stop();
    await emailer?.stop();
    await schemaRegistry?.stop();
    await kafka?.stop();
    await network?.stop();
  });

  it('messageIsProcessed', async () => {
    await whenMessageArrivesOnKafka();

    await thenEmailerIsCalled();
  });

  async function whenMessageArrivesOnKafka(): Promise<void> {
    const value = await registry.encode(schemaId, SUGGESTION_MESSAGE);
    await produce(brokers, SUGGESTIONS_TOPIC, value);
  }

  async function thenEmailerIsCalled(): Promise<void> {
    await vi.waitFor(
      () => {
        const calls = emailer.requests().filter((r) => r.path === EMAIL_PATH);
        expect(calls).toHaveLength(1);
        expect(calls[0]!.method).toBe('POST');
        expect(calls[0]!.contentType).toBe('application/json');
        expect(calls[0]!.accept).toBe('application/json');
        expect(calls[0]!.body).toEqual(EXPECTED_EMAIL);
      },
      { timeout: 5_000, interval: 100 },
    );
  }
});
