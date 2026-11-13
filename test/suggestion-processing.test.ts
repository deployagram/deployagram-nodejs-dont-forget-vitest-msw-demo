import { http, passthrough } from 'msw';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import type { StartedTestContainer } from 'testcontainers';
import { startDeployagramEnvironment, stopDeployagramEnvironment } from '@deployagram/environment';
import { DiagramLoggerClient } from '@deployagram/logger-client';
import { recordConsumedMessages } from '@deployagram/kafkajs';
import { recordHttp, type HttpRecorder } from '@deployagram/msw';
import { startApplication, type RunningApplication } from '../src/app.js';
import {
  brokersOf,
  createTopic,
  networkBootstrapServersOf,
  produce,
  SUGGESTIONS_TOPIC,
  startKafka,
} from './support/kafka.js';
import { EMAIL_PATH, EMAILER_BASE_URL, EmailerDouble } from './support/emailer-double.js';
import {
  networkSchemaRegistryUrlOf,
  registerSuggestionSchema,
  schemaRegistryUrlOf,
  startSchemaRegistry,
} from './support/schema-registry.js';

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
  let kafka: StartedKafkaContainer;
  let schemaRegistry: StartedTestContainer;
  let registry: SchemaRegistry;
  let schemaId: number;
  let emailer: EmailerDouble;
  let application: RunningApplication;
  let brokers: string[];
  let recorder: HttpRecorder;
  let logger: DiagramLoggerClient;

  beforeAll(async () => {
    // Finds the running Collector, or starts one on the Deployagram network. The runtime modes let the
    // test point the Collector at its broker and registry.
    const deployagram = await startDeployagramEnvironment({
      loggerEnvironment: { KAFKA_BROKER_LOCATION_MODE: 'RUNTIME', SCHEMA_REGISTRY_LOCATION_MODE: 'RUNTIME' },
    });
    const COLLECTOR_URL = deployagram.logger.url;
    logger = new DiagramLoggerClient({ baseUrl: COLLECTOR_URL });

    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `dontforget-kafka-${suffix}`;
    const registryAlias = `dontforget-schema-registry-${suffix}`;

    kafka = await startKafka(kafkaAlias);
    brokers = brokersOf(kafka);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(registryAlias, kafkaAlias);
    const schemaRegistryUrl = schemaRegistryUrlOf(schemaRegistry);
    registry = new SchemaRegistry({ host: schemaRegistryUrl });
    schemaId = await registerSuggestionSchema(registry);

    // The Collector reads each consumed record from the broker and decodes it with the registry.
    // It reaches them by their names on the Deployagram network.
    await logger.configureKafkaBroker(networkBootstrapServersOf(kafkaAlias));
    await logger.configureSchemaRegistry(networkSchemaRegistryUrlOf(registryAlias));

    emailer = new EmailerDouble();
    // The recorder and the Schema Registry client both make HTTP calls. Let those reach their
    // servers, rather than treating them as unhandled.
    emailer.server.use(
      http.all(`${COLLECTOR_URL}/*`, () => passthrough()),
      http.all(`${schemaRegistryUrl}/*`, () => passthrough()),
    );
    await emailer.start();

    recorder = recordHttp(emailer.server, {
      logger,
      caller: 'DontForget',
      callees: [{ name: 'ORG_ctm_01m2zttafctbr81r36aem7z8qw:Emailer', urlPrefix: EMAILER_BASE_URL }],
    });

    application = await startApplication({
      kafkaBrokers: brokers,
      schemaRegistryUrl,
      emailerBaseUrl: EMAILER_BASE_URL,
      wrapEachMessage: (handler) => recordConsumedMessages({ logger, consumingApp: 'DontForget' }, handler),
    });
  }, 180_000);

  afterEach(async (context) => {
    await recorder.flush();
    await logger.markEnd(`SuggestionProcessingTest.${context.task.name}`);
  });

  afterAll(async () => {
    recorder?.stop();
    await application?.stop();
    await emailer?.stop();
    await schemaRegistry?.stop();
    await kafka?.stop();
    // Last: stopping containers goes through the Docker API, which MSW would intercept while the
    // Emailer double is still listening.
    await stopDeployagramEnvironment();
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
