module.exports = {
  apps: [{
    name: "ichibi-lake-gw",
    script: "./src/index.js",
    exec_mode: "fork",
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: "1G",
    env: {
      NODE_ENV: "development",
      PORT: 3000,
      API_KEYS: "ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1,ICHIBI_LAKE_RESEARCH_035a2116-c9d0-4603-9f12-da1fb57294d1,ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1,ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13",
      DUCKLAKE_PG_HOST: "0.0.0.0",
      DUCKLAKE_PG_PORT: "5432",
      DUCKLAKE_PG_USER: "postgres",
      DUCKLAKE_PG_PASSWORD: "postgres",
      DUCKLAKE_PG_DB: "ducklake",

      // Kafka Internal Consumer Settings
      // KAFKA_BROKERS to supply brokers to enable internal background consumer
      KAFKA_BROKERS: "localhost:9092,broker2:9092",
      KAFKA_CLIENT_ID: "ichibi-lake-consumer",
      KAFKA_GROUP_ID: "ichibi-lake-ingestion-group",
      KAFKA_TOPIC_REGEX: ".*",
      KAFKA_BATCH_SIZE: 100,
      KAFKA_BATCH_TIMEOUT_MS: 5000
    },
    env_production: {
      NODE_ENV: "production",
      PORT: 3000
    }
  }]
};
