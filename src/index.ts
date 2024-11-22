import makeWASocket, { useMultiFileAuthState, DisconnectReason, WAMessage, WASocket } from '@whiskeysockets/baileys';
import qrcode from 'qrcode-terminal';
import { Boom } from '@hapi/boom';
import amqp from 'amqplib';

// RabbitMQ Configuration
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_NAME = 'WhatsBotExchange'; // Define an exchange
let rabbitChannel: amqp.Channel | null = null;
let sock: WASocket | null = null;

// Connect to RabbitMQ and setup the exchange and queues
async function connectRabbitMQ() {
    const connection = await amqp.connect(RABBITMQ_URL);
    rabbitChannel = await connection.createChannel();

    // Declare the exchange
    await rabbitChannel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });


    console.log('Connected to RabbitMQ');
}

// Publish to the exchange with a routing key
async function publishToRabbitMQ(routingKey: string, message: object) {
    if (!rabbitChannel) {
        console.error('RabbitMQ channel is not initialized');
        return;
    }

    rabbitChannel.publish(
        EXCHANGE_NAME,
        routingKey,
        Buffer.from(JSON.stringify(message)),
        { persistent: true }
    );
    console.log(`Published to exchange ${EXCHANGE_NAME} with routing key ${routingKey}:`, message);
}

// Listen to messages from the `WhatsComs` queue
async function consumeFromWhatsComs() {
    if (!rabbitChannel) {
        throw new Error("RabbitMQ channel is not initialized");
    }

    console.log("Waiting for messages in WhatsComs...");
    await rabbitChannel.consume('WhatsComs', async (msg) => {
        if (msg) {
            try {
                const payload = JSON.parse(msg.content.toString());
                console.log("Received message from WhatsComs:", payload);

                const { jid = payload.remoteJid, text = payload.data.text, type = payload.type } = payload;

                // Send message via WhatsApp
                if (sock && jid && text && type != 'data') {
                    await sock.sendMessage(jid, { text });
                    console.log(`Message sent to ${jid}: ${text}`);
                }

                // Acknowledge message
                rabbitChannel!.ack(msg);
            } catch (error) {
                console.error("Error processing WhatsComs message:", error);
                rabbitChannel!.nack(msg, false, false); // Reject without requeue
            }
        }
    });
}

// Connect to WhatsApp and handle incoming messages
async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
    sock = makeWASocket({ auth: state });

    sock.ev.on('connection.update', (update) => {
        const { connection, qr, lastDisconnect } = update;

        if (qr) qrcode.generate(qr, { small: true });

        if (connection === 'close') {
            const error = (lastDisconnect?.error as Boom)?.output?.statusCode;
            const shouldReconnect = error !== DisconnectReason.loggedOut;

            console.log('Connection closed:', error, ', reconnecting:', shouldReconnect);
            if (shouldReconnect) connectToWhatsApp();
            else console.error('Logged out. Please restart the bot.');
        } else if (connection === 'open') {
            console.log('WhatsApp connection opened successfully!');
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async (msg) => {
        const message = msg.messages[0] as WAMessage;
        const remoteJid = message.key.remoteJid ?? '';
        const text = message.message?.conversation || message.message?.extendedTextMessage?.text || '';

        console.log("Object Data (JSON):", JSON.stringify(message, null, 2));
        // Determine which queue to publish to
        let payload = {};
        let routingKey = '';
        if (message.message?.audioMessage) {
            const base64String = btoa(String.fromCharCode(...message.message.audioMessage.mediaKey!));
            routingKey = 'data.audiotrans';
            payload = {
                remoteJid,
                data: {
                    url: message.message.audioMessage.url,
                    mediakey: base64String,
                },
                timestamp: message.messageTimestamp,
            };
        } else if (remoteJid === '120363356507455451@g.us' && message.key.fromMe == false) {
            routingKey = 'data.weatherapi';
            payload = {
                remoteJid,
                data: {
                    text,
                },
                type: 'data',
                timestamp: message.messageTimestamp,
            };
        }else if(message.key.fromMe == false) {
            routingKey = 'data.extension';
            payload = {
                remoteJid,
                data: {
                    text,
                },
                type: 'data',
                timestamp: message.messageTimestamp,
            };
        }

        // Publish to the exchange with the routing key
        if (rabbitChannel) {
            await publishToRabbitMQ(routingKey, payload);
        }
    });
}

// Initialize RabbitMQ and WhatsApp Bot
(async () => {
    try {
        await connectRabbitMQ();
        await connectToWhatsApp();

        // Start consuming messages from WhatsComs queue
        await consumeFromWhatsComs();
    } catch (err) {
        console.error('Error during initialization:', err);
    }
})();
