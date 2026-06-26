import { randomUUID } from 'crypto';

const API_KEY = 'ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1';
const BASE_URL = process.env.BASE_URL || 'http://impi:3000';
const RECORD_COUNT = 1000000;

async function runStressTest() {
    console.log(`Generating ${RECORD_COUNT} records for upload...`);
    const uploadData = [];
    for (let i = 0; i < RECORD_COUNT; i++) {
        uploadData.push({ id: i, uuid: randomUUID(), name: `duck_${i}`, value: Math.random() * 100 });
    }

    console.log(`Sending upload request to ${BASE_URL}/upload/stress_test_table...`);
    const startUpload = Date.now();
    const uploadRes = await fetch(`${BASE_URL}/upload/stress_test_table`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify(uploadData)
    });

    if (!uploadRes.ok) throw new Error(`Upload failed: ${uploadRes.status} ${uploadRes.statusText}`);
    console.log('Upload Response:', await uploadRes.json());
    console.log(`Upload completed in ${Date.now() - startUpload}ms\n`);

    console.log(`Generating ${RECORD_COUNT} records for kafka-sink...`);
    const kafkaData = [];
    for (let i = 0; i < RECORD_COUNT; i++) {
        kafkaData.push({ key: `msg_${i}`, value: { event: "stress_test", user: `user_${i}`, timestamp: Date.now() } });
    }

    console.log(`Sending upload request to ${BASE_URL}/kafka-sink...`);
    const startKafka = Date.now();
    const kafkaSinkRes = await fetch(`${BASE_URL}/kafka-sink`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Kafka-Topic': 'stress_kafka_events',
            'x-api-key': API_KEY
        },
        body: JSON.stringify(kafkaData)
    });

    if (!kafkaSinkRes.ok) throw new Error(`Kafka sink failed: ${kafkaSinkRes.status} ${kafkaSinkRes.statusText}`);
    console.log('Kafka Sink Response:', await kafkaSinkRes.json());
    console.log(`Kafka sink completed in ${Date.now() - startKafka}ms\n`);

    console.log('Validating data via query...');
    const queryUploadRes = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: "SELECT COUNT(*) as records FROM stress_test_table" })
    });

    if (!queryUploadRes.ok) throw new Error(`Query failed: ${queryUploadRes.statusText}`);
    console.log('Total Rows in stress_test_table:', await queryUploadRes.json());

    const queryKafkaRes = await fetch(`${BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: JSON.stringify({ sql: "SELECT COUNT(*) as records FROM stress_kafka_events" })
    });

    if (!queryKafkaRes.ok) throw new Error(`Query failed: ${queryKafkaRes.statusText}`);
    console.log('Total Rows in stress_kafka_events:', await queryKafkaRes.json());
}

runStressTest().catch(err => {
    console.error('Stress test failed:', err);
    process.exit(1);
});
