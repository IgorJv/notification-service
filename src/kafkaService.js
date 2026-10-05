const { Kafka } = require('kafkajs');

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'notification-service';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'notification-group';
const TOPICS = process.env.KAFKA_TOPICS
    ? process.env.KAFKA_TOPICS.split(',')
    : ['order-events', 'account-events'];

const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS,
    retry: {
        initialRetryTime: 300,
        retries: 10
    }
});

const consumer = kafka.consumer({
    groupId: KAFKA_GROUP_ID,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
});

const run = async () => {
    try {
        await consumer.connect();
        console.log(`[NotificationService] Successfully connected to Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);

        await consumer.subscribe({topics: TOPICS, fromBeginning: false});
        console.log(`[NotificationService] Subscribed to topics: ${TOPICS.join(', ')}`);

        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                const rawMessage =  message.value ? message.value.toString() : null;
                if (!rawMessage) {
                    console.warn(`[NotificationService] Received empty message on partition ${partition}`);
                    return;
                }

                try {
                    const data = JSON.parse(rawMessage);

                    if (topic === 'order-events') {
                        console.log(JSON.stringify({
                            level: 'INFO',
                            service: KAFKA_CLIENT_ID,
                            event: 'ORDER_EVENT_PROCESSED',
                            timestamp: new Date().toISOString(),
                            topic,
                            partition,
                            offset: message.offset,
                            userId: data.userId,
                            orderId: data.orderId || 'N/A'
                        }));
                    } else if (topic === 'account-events') {
                        console.log(JSON.stringify({
                            level: 'INFO',
                            service: KAFKA_CLIENT_ID,
                            event: 'ACCOUNT_EVENT_PROCESSED',
                            timestamp: new Date().toISOString(),
                            topic,
                            partition,
                            offset: message.offset,
                            accountId: data.accountId,
                            amount: data.amount,
                            status: data.status
                        }));
                    }
                } catch (error) {
                    console.error(JSON.stringify({
                        level: 'ERROR',
                        service: KAFKA_CLIENT_ID,
                        event: 'MESSAGE_PARSE_FAILED',
                        error: error.message,
                        rawPayload: rawMessage
                    }));
                }
            }
        });
    } catch (error) {
        console.error('[NotificationService] Fatal error in Kafka consumer loop:', error);
        process.exit(1);
    }
};

const shutdown = async (signal) => {
    console.log(`[NotificationService] Received ${signal}. Shutting down gracefully...`);
    try {
        await consumer.disconnect();
        console.log('[NotificationService] Kafka consumer disconnected.');
        process.exit(0);
    } catch (error) {
        console.error('[NotificationService] Error during graceful shutdown:', err);
        process.exit(1);
    }
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

run().catch((err) => {
    console.error('[NotificationService] Unhandled startup error:', err);
    process.exit(1);
});