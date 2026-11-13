import { startApplication } from './app.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const schemaRegistryUrl = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
const emailerBaseUrl = process.env.EMAILER_BASE_URL ?? 'http://localhost:9001';

const application = await startApplication({ kafkaBrokers: brokers, schemaRegistryUrl, emailerBaseUrl });
console.info(`Reading ${brokers.join(',')} with schemas from ${schemaRegistryUrl}, emailing via ${emailerBaseUrl}`);

const shutdown = (): void => {
  application.stop().finally(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
