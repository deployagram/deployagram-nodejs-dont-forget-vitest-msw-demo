# Deployagram Node.js Don't Forget demo (Vitest, MSW)

The application reads suggestions from a Kafka topic, `ORG_ctm_01m1j08sx96792d2tysabvyadp__COLON__SuppliedSuggestionsTopic`,
and sends an email for each one by calling the Emailer over HTTP (`POST /Emailer/email`). Each message is Avro, with its
schema held in Confluent Schema Registry.

The service test calls the Emailer double, which answers in-process through MSW, so no HTTP server is needed.

## Tests

```bash
npm install
npm test
```

Docker must be running. The service test starts Kafka and the Schema Registry, sends a suggestion to the topic, and
checks that the application calls the Emailer with the right email.
