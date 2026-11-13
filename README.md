# Deployagram Node.js Don't Forget demo (Vitest)

The application reads suggestions from a Kafka topic, `ORG_ctm_01m1j08sx96792d2tysabvyadp__COLON__SuppliedSuggestionsTopic`, and sends an
email for each one by calling the Emailer over HTTP (`POST /Emailer/email`). Each message is Avro,
with its schema held in Confluent Schema Registry.

## Status

- [x] Service test: `test/suggestion-processing.test.ts`.
- [x] Application: `src/app.ts` reads the topic with `kafkajs`, decodes each Avro message with the
  Schema Registry, and posts each email to the Emailer.
- [x] Kafka instrumentation: the service test reports each consumed record to the Collector with
  `@deployagram/kafkajs`. The Collector reads the record from the broker and decodes it.
- [x] No Deployagram code in the application: the service test wraps the consumer and the Emailer calls.

## Running the application

```bash
npm install
npm run build
KAFKA_BROKERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 EMAILER_BASE_URL=http://localhost:9001 node dist/main.js
```

## Recording the calls

The service test records the HTTP calls between the application and the Emailer with
`@deployagram/msw`. The recorder sends each request and response to the Collector, under the name of
the test, so the test's recording can be read there as it is for the Proxy.

The test finds the Collector, or starts one with `@deployagram/environment`:

1. If `DEPLOYAGRAM_LOGGER_URL` is set, that Collector is used, and it must be reachable.
2. Otherwise a running Collector container is reused.
3. Otherwise the test starts one on the `deployagram` Docker network, in runtime configuration mode.

Either way, the test lets the Collector's requests pass through MSW, so they are not treated as
unhandled. The test points the Collector at its Kafka broker and Schema Registry, so a Collector
reused from elsewhere must be in runtime mode (`kafka.broker.location.mode` and
`schema.registry.location.mode` set to `RUNTIME`). In static mode it answers `409`, and the test fails.

## Tests

The service test starts Kafka and Schema Registry containers with testcontainers, on one network.
It registers the suggestion's Avro schema, writes an Avro suggestion to the topic, and checks that
the Emailer double received the expected email. The Emailer is stubbed with MSW.

```bash
npm install
npm test
```

Docker must be running. The `.env` file holds the Deployagram licence key and cloud credentials, and
the `.deployagram` licence file is read from this folder. Both are kept out of git. They are needed
only when the test starts a Collector.

The environment package also starts a Proxy container, which this demo does not use. That is an extra
container each run, until the package can skip it.

## Decisions

- **Kafka image.** `apache/kafka-native` does not work with testcontainers' Kafka module, which
  starts the broker with `/etc/confluent/docker/run`. That script is only in Confluent's images, so
  the tests use `confluentinc/cp-kafka:7.6.1` in KRaft mode.

- **Avro with Confluent Schema Registry.** The message follows `test/resources/avro/dont_forget_message.avsc`,
  the schema the Java Don't Forget demo uses. The value is registered under `ORG_ctm_01m1j08sx96792d2tysabvyadp__COLON__SuppliedSuggestionsTopic-value`,
  the Confluent subject name. The application decodes it with `@kafkajs/confluent-schema-registry`.

- **The recipient is in the request body.** The Emailer is called as `POST /Emailer/email`, with
  the email address in the JSON body.
