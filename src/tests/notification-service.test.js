const { Kafka } = require('kafkajs');

const mockConsumerInstance = {
    connect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn(),
    disconnect: jest.fn().mockResolvedValue(undefined)
};

jest.mock('kafkajs', () => {
    return {
        Kafka: jest.fn().mockImplementation(() => {
            return {
                consumer: jest.fn(() => mockConsumerInstance)
            };
        })
    };
});

describe('Notification Service Kafka Consumer Tests', () => {
    let consoleLogSpy;
    let consoleErrorSpy;
    let consoleWarnSpy;
    let eachMessageCallback;

    beforeEach(() => {
        jest.clearAllMocks();
        jest.resetModules();

        consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
        consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
        consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

        /** @type {jest.Mock} */
        (mockConsumerInstance.run).mockImplementation(async ({ eachMessage }) => {
            eachMessageCallback = eachMessage;
        });
    });

    afterEach(() => {
        consoleLogSpy.mockRestore();
        consoleErrorSpy.mockRestore();
        consoleWarnSpy.mockRestore();
    });

    test('should successfully connect and subscribe to Kafka topics on startup', async () => {
        require('../main/kafkaService');
        await new Promise(setImmediate);

        expect(mockConsumerInstance.connect).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.subscribe).toHaveBeenCalledWith({
            topics: ['order-events', 'account-events'],
            fromBeginning: false
        });
        expect(mockConsumerInstance.run).toHaveBeenCalledTimes(1);
    });

    test('should process order-events topic message and log structured info', async () => {
        require('../main/kafkaService');
        await new Promise(setImmediate);

        const payload = {
            userId: 'user-123',
            orderId: 'ord-999'
        };

        const mockMessage = {
            topic: 'order-events',
            partition: 0,
            message: {
                offset: '10',
                value: Buffer.from(JSON.stringify(payload))
            }
        };

        await eachMessageCallback(mockMessage);

        expect(consoleLogSpy).toHaveBeenCalled();
        const loggedOutput = JSON.parse(consoleLogSpy.mock.calls.find(call =>
            call[0].includes('ORDER_EVENT_PROCESSED')
        )[0]);

        expect(loggedOutput.event).toBe('ORDER_EVENT_PROCESSED');
        expect(loggedOutput.userId).toBe('user-123');
        expect(loggedOutput.orderId).toBe('ord-999');
        expect(loggedOutput.topic).toBe('order-events');
    });

    test('should process account-events topic message and log structured info', async () => {
        require('../main/kafkaService');
        await new Promise(setImmediate);

        const payload = {
            accountId: 555,
            amount: 1500.50,
            status: 'SUCCESS'
        };

        const mockMessage = {
            topic: 'account-events',
            partition: 1,
            message: {
                offset: '42',
                value: Buffer.from(JSON.stringify(payload))
            }
        };

        await eachMessageCallback(mockMessage);

        const loggedOutput = JSON.parse(consoleLogSpy.mock.calls.find(call =>
            call[0].includes('ACCOUNT_EVENT_PROCESSED')
        )[0]);

        expect(loggedOutput.event).toBe('ACCOUNT_EVENT_PROCESSED');
        expect(loggedOutput.accountId).toBe(555);
        expect(loggedOutput.amount).toBe(1500.50);
        expect(loggedOutput.status).toBe('SUCCESS');
    });

    test('should handle empty or null messages gracefully with a warning', async () => {
        require('../main/kafkaService');
        await new Promise(setImmediate);

        const mockMessage = {
            topic: 'order-events',
            partition: 0,
            message: {
                offset: '11',
                value: null
            }
        };

        await eachMessageCallback(mockMessage);

        expect(consoleWarnSpy).toHaveBeenCalledWith(
            expect.stringContaining('Received empty message on partition 0')
        );
    });

    test('should catch JSON parsing errors and log error payload', async () => {
        require('../main/kafkaService');
        await new Promise(setImmediate);

        const invalidJsonString = '{ invalid-json-syntax';

        const mockMessage = {
            topic: 'order-events',
            partition: 0,
            message: {
                offset: '12',
                value: Buffer.from(invalidJsonString)
            }
        };

        await eachMessageCallback(mockMessage);

        expect(consoleErrorSpy).toHaveBeenCalled();
        const errorOutput = JSON.parse(consoleErrorSpy.mock.calls.find(call =>
            call[0].includes('MESSAGE_PARSE_FAILED')
        )[0]);

        expect(errorOutput.event).toBe('MESSAGE_PARSE_FAILED');
        expect(errorOutput.rawPayload).toBe(invalidJsonString);
    });
});
