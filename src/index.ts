import makeWASocket, {
    DisconnectReason,
    downloadMediaMessage,
    fetchLatestBaileysVersion,
    fetchLatestWaWebVersion,
    useMultiFileAuthState,
    type AnyMessageContent,
    type WAMediaUpload,
    type WAMessage,
    type WAVersion,
    type WASocket,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import amqp, { type ConsumeMessage } from 'amqplib';
import dotenv from 'dotenv';
import { createRequire } from 'node:module';

dotenv.config();

const require = createRequire(import.meta.url);
const qrcode = require('qrcode-terminal') as typeof import('qrcode-terminal');

type JsonRecord = Record<string, unknown>;
type OutgoingMessageType = 'text' | 'image' | 'audio' | 'document';
type IncomingMessageType = 'text' | 'image' | 'audio' | 'document' | 'unknown';

interface NormalizedOutgoingJob {
    destinations: string[];
    type: OutgoingMessageType;
    content: AnyMessageContent;
}

interface IncomingMediaPayload {
    url?: string;
    mediaUrl?: string;
    directPath?: string;
    mediaKey?: string;
    mimetype?: string;
    fileName?: string;
    fileLength?: number;
    caption?: string;
    seconds?: number;
    ptt?: boolean;
    base64?: string;
    mediaBase64?: string;
}

interface IncomingGroupPayload {
    jid: string;
    subject?: string;
    participantCount?: number;
}

interface IncomingWhatsAppMessage {
    event: 'whatsapp.message.received';
    id?: string;
    remoteJid: string;
    senderJid: string;
    fromMe: boolean;
    type: IncomingMessageType;
    text?: string;
    media?: IncomingMediaPayload;
    group?: IncomingGroupPayload;
    timestamp: number;
    pushName?: string;
}

class ValidationError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ValidationError';
    }
}

class TransientProcessingError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'TransientProcessingError';
    }
}

function getEnv(name: string, fallback: string): string {
    const value = process.env[name];
    return value && value.trim().length > 0 ? value : fallback;
}

function getPositiveIntEnv(name: string, fallback: number): number {
    const parsed = Number.parseInt(process.env[name] ?? '', 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const RABBITMQ_URL = getEnv('RABBITMQ_URL', 'amqp://localhost');
const WHATSAPP_AUTH_DIR = getEnv('WHATSAPP_AUTH_DIR', 'auth_info_baileys');
const WHATSAPP_WEB_VERSION = process.env.WHATSAPP_WEB_VERSION;
const DOWNLOAD_INCOMING_DOCUMENTS = getEnv('WHATSAPP_DOWNLOAD_INCOMING_DOCUMENTS', 'true').toLowerCase() !== 'false';
const OUTGOING_QUEUE_NAME = getEnv('WHATSAPP_OUTGOING_QUEUE', 'WhatsComs');
const OUTGOING_PREFETCH = getPositiveIntEnv('WHATSAPP_OUTGOING_PREFETCH', 1);
const SEND_WAIT_TIMEOUT_MS = getPositiveIntEnv('WHATSAPP_SEND_WAIT_TIMEOUT_MS', 60_000);

const INCOMING_EXCHANGE_NAME = getEnv('WHATSAPP_INCOMING_EXCHANGE', 'WhatsBotExchange');
const INCOMING_EXCHANGE_TYPE = getEnv('WHATSAPP_INCOMING_EXCHANGE_TYPE', 'direct');
const INCOMING_QUEUE_NAME = getEnv('WHATSAPP_INCOMING_QUEUE', 'WhatsIncoming');
const INCOMING_ROUTING_KEY_PREFIX = getEnv('WHATSAPP_INCOMING_ROUTING_KEY_PREFIX', 'whatsapp.incoming');
const INCOMING_MESSAGE_TYPES: IncomingMessageType[] = ['text', 'image', 'audio', 'document', 'unknown'];

let rabbitConnection: amqp.Connection | null = null;
let rabbitChannel: amqp.Channel | null = null;
let sock: WASocket | null = null;
let whatsappConnected = false;
let connectionWaiters: Array<() => void> = [];
let whatsappVersionPromise: Promise<WAVersion> | null = null;
const groupMetadataCache = new Map<string, IncomingGroupPayload>();

function isRecord(value: unknown): value is JsonRecord {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function getString(record: JsonRecord, ...keys: string[]): string | undefined {
    for (const key of keys) {
        const value = record[key];
        if (typeof value === 'string' && value.trim().length > 0) {
            return value.trim();
        }
    }

    return undefined;
}

function getBoolean(record: JsonRecord, key: string): boolean | undefined {
    const value = record[key];
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (normalized === 'true') return true;
        if (normalized === 'false') return false;
    }

    return undefined;
}

function toOptionalNumber(value: unknown): number | undefined {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim().length > 0) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
    }
    if (isRecord(value) && typeof value.toNumber === 'function') {
        const parsed = value.toNumber();
        return Number.isFinite(parsed) ? parsed : undefined;
    }

    return undefined;
}

function toUnixTimestamp(value: unknown): number {
    return toOptionalNumber(value) ?? Math.floor(Date.now() / 1000);
}

function bytesToBase64(bytes?: Uint8Array | null): string | undefined {
    return bytes ? Buffer.from(bytes).toString('base64') : undefined;
}

function mergeRecord(base: JsonRecord, override: JsonRecord): JsonRecord {
    return { ...base, ...override };
}

function normalizeDestinations(job: JsonRecord): string[] {
    const values: unknown[] = [job.jid, job.remoteJid, job.destination];

    if (Array.isArray(job.destinations)) {
        values.push(...job.destinations);
    } else {
        values.push(job.destinations);
    }

    if (Array.isArray(job.jids)) {
        values.push(...job.jids);
    }

    return [...new Set(values.filter((value): value is string => typeof value === 'string')
        .map((value) => value.trim())
        .filter(Boolean))];
}

function getOutgoingMessageRecord(job: JsonRecord): JsonRecord {
    if (isRecord(job.message)) return job.message;
    if (isRecord(job.data)) return job.data;
    return job;
}

function normalizeOutgoingType(rawType: string | undefined, message: JsonRecord): OutgoingMessageType {
    const normalized = rawType?.trim().toLowerCase();

    if (normalized === 'text') return 'text';
    if (normalized === 'image' || normalized === 'img') return 'image';
    if (normalized === 'audio' || normalized === 'voice') return 'audio';
    if (normalized === 'document' || normalized === 'file' || normalized === 'zip') return 'document';

    if (getString(message, 'text', 'body')) {
        return 'text';
    }

    throw new ValidationError('Outgoing job must include type text, image, or audio.');
}

function stripBase64Metadata(value: string): string {
    return value.replace(/^data:[^;]+;base64,/, '');
}

function buildMediaUpload(message: JsonRecord): WAMediaUpload {
    const mediaRecord = isRecord(message.media) ? mergeRecord(message, message.media) : message;
    const mediaBase64 = getString(mediaRecord, 'mediaBase64', 'base64');
    const mediaUrl = getString(mediaRecord, 'mediaUrl', 'url');

    if (mediaBase64) return Buffer.from(stripBase64Metadata(mediaBase64), 'base64');
    if (mediaUrl) return { url: mediaUrl };

    throw new ValidationError('Media jobs must include mediaUrl, url, mediaBase64, or base64.');
}

function buildOutgoingContent(type: OutgoingMessageType, message: JsonRecord): AnyMessageContent {
    const text = getString(message, 'text', 'body');
    const caption = getString(message, 'caption');
    const mimetype = getString(message, 'mimetype', 'mimeType');
    const fileName = getString(message, 'fileName', 'filename', 'name');

    if (type === 'text') {
        if (!text) throw new ValidationError('Text jobs must include text or body.');
        return { text };
    }

    const media = buildMediaUpload(message);

    if (type === 'image') {
        return {
            image: media,
            ...(caption ? { caption } : {}),
            ...(mimetype ? { mimetype } : {}),
        };
    }

    if (type === 'document') {
        return {
            document: media,
            mimetype: mimetype ?? 'application/octet-stream',
            fileName: fileName ?? 'file',
            ...(caption ? { caption } : {}),
        };
    }

    return {
        audio: media,
        mimetype: mimetype ?? 'audio/mpeg',
        ptt: getBoolean(message, 'ptt') ?? false,
        ...(toOptionalNumber(message.seconds) ? { seconds: toOptionalNumber(message.seconds) } : {}),
    };
}

function normalizeOutgoingJob(value: unknown): NormalizedOutgoingJob {
    if (!isRecord(value)) {
        throw new ValidationError('Outgoing job must be a JSON object.');
    }

    const destinations = normalizeDestinations(value);
    if (destinations.length === 0) {
        throw new ValidationError('Outgoing job must include jid, remoteJid, destination, destinations, or jids.');
    }

    const messageRecord = getOutgoingMessageRecord(value);
    const message = mergeRecord(value, messageRecord);
    const type = normalizeOutgoingType(getString(message, 'type'), message);

    return {
        destinations,
        type,
        content: buildOutgoingContent(type, message),
    };
}

function parseJsonPayload(buffer: Buffer): unknown {
    try {
        return JSON.parse(buffer.toString('utf8'));
    } catch {
        throw new ValidationError('Queue message body must be valid JSON.');
    }
}

function isGroupJid(jid: string): boolean {
    return jid.endsWith('@g.us');
}

async function getGroupPayload(remoteJid: string): Promise<IncomingGroupPayload | undefined> {
    if (!isGroupJid(remoteJid)) return undefined;

    const cached = groupMetadataCache.get(remoteJid);
    if (cached) return cached;

    if (!sock) {
        return { jid: remoteJid };
    }

    try {
        const metadata = await sock.groupMetadata(remoteJid);
        const group = {
            jid: remoteJid,
            subject: metadata.subject,
            participantCount: metadata.size ?? metadata.participants.length,
        };

        groupMetadataCache.set(remoteJid, group);
        return group;
    } catch (error) {
        console.error(`Could not load group metadata for ${remoteJid}:`, error);
        return { jid: remoteJid };
    }
}

function parseWhatsAppVersion(value: string | undefined): WAVersion | null {
    if (!value) return null;

    const version = value.split(',').map((part) => Number.parseInt(part.trim(), 10));
    if (version.length !== 3 || version.some((part) => !Number.isFinite(part))) {
        throw new Error('WHATSAPP_WEB_VERSION must use the format "2,3000,1234567890".');
    }

    return version as WAVersion;
}

function getWhatsAppVersion(): Promise<WAVersion> {
    if (!whatsappVersionPromise) {
        whatsappVersionPromise = (async () => {
            const envVersion = parseWhatsAppVersion(WHATSAPP_WEB_VERSION);
            if (envVersion) {
                console.log(`Using WhatsApp Web version from env: ${envVersion.join('.')}`);
                return envVersion;
            }

            const waWebVersion = await fetchLatestWaWebVersion({ timeout: 10_000 });
            if (waWebVersion.isLatest) {
                console.log(`Using latest WhatsApp Web version: ${waWebVersion.version.join('.')}`);
                return waWebVersion.version;
            }

            console.warn('Could not fetch latest WhatsApp Web version. Falling back to Baileys master version.', waWebVersion.error);
            const baileysVersion = await fetchLatestBaileysVersion({ timeout: 10_000 });
            console.log(`Using Baileys version: ${baileysVersion.version.join('.')} latest: ${baileysVersion.isLatest}`);
            return baileysVersion.version;
        })();
    }

    return whatsappVersionPromise;
}

function incomingRoutingKey(type: IncomingMessageType): string {
    return `${INCOMING_ROUTING_KEY_PREFIX}.${type}`;
}

function incomingBindingKeys(): string[] {
    if (INCOMING_EXCHANGE_TYPE === 'fanout') return [''];
    if (INCOMING_EXCHANGE_TYPE === 'topic') return [`${INCOMING_ROUTING_KEY_PREFIX}.#`];

    return INCOMING_MESSAGE_TYPES.map((type) => incomingRoutingKey(type));
}

async function connectRabbitMQ(): Promise<void> {
    rabbitConnection = await amqp.connect(RABBITMQ_URL);
    rabbitConnection.on('error', (error) => console.error('RabbitMQ connection error:', error));
    rabbitConnection.on('close', () => console.error('RabbitMQ connection closed'));

    rabbitChannel = await rabbitConnection.createChannel();
    rabbitChannel.on('error', (error) => console.error('RabbitMQ channel error:', error));

    await rabbitChannel.assertQueue(OUTGOING_QUEUE_NAME, { durable: true });
    await rabbitChannel.prefetch(OUTGOING_PREFETCH);

    await rabbitChannel.assertExchange(INCOMING_EXCHANGE_NAME, INCOMING_EXCHANGE_TYPE, { durable: true });
    await rabbitChannel.assertQueue(INCOMING_QUEUE_NAME, { durable: true });

    for (const bindingKey of incomingBindingKeys()) {
        await rabbitChannel.bindQueue(INCOMING_QUEUE_NAME, INCOMING_EXCHANGE_NAME, bindingKey);
    }

    console.log(`Connected to RabbitMQ at ${RABBITMQ_URL}`);
    console.log(`Consuming outbound WhatsApp jobs from queue "${OUTGOING_QUEUE_NAME}"`);
    console.log(`Publishing incoming WhatsApp messages to exchange "${INCOMING_EXCHANGE_NAME}"`);
}

function publishIncomingMessage(payload: IncomingWhatsAppMessage): void {
    if (!rabbitChannel) {
        throw new TransientProcessingError('RabbitMQ channel is not initialized.');
    }

    const routingKey = incomingRoutingKey(payload.type);
    rabbitChannel.publish(
        INCOMING_EXCHANGE_NAME,
        routingKey,
        Buffer.from(JSON.stringify(payload)),
        {
            contentType: 'application/json',
            messageId: payload.id,
            persistent: true,
            timestamp: Date.now(),
            type: 'whatsapp.incoming',
        },
    );

    console.log(`Published incoming ${payload.type} message from ${payload.senderJid} with routing key "${routingKey}"`);
}

function setWhatsAppConnected(connected: boolean): void {
    whatsappConnected = connected;

    if (!connected) return;

    const waiters = connectionWaiters;
    connectionWaiters = [];
    waiters.forEach((resolve) => resolve());
}

async function waitForWhatsAppConnection(): Promise<void> {
    if (whatsappConnected && sock) return;

    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
            connectionWaiters = connectionWaiters.filter((candidate) => candidate !== resolveConnection);
            reject(new TransientProcessingError('WhatsApp is not connected yet.'));
        }, SEND_WAIT_TIMEOUT_MS);

        const resolveConnection = () => {
            clearTimeout(timeout);
            resolve();
        };

        connectionWaiters.push(resolveConnection);
    });
}

async function sendOutgoingJob(value: unknown): Promise<void> {
    const job = normalizeOutgoingJob(value);
    await waitForWhatsAppConnection();

    if (!sock) {
        throw new TransientProcessingError('WhatsApp socket is not initialized.');
    }

    for (const destination of job.destinations) {
        await sock.sendMessage(destination, job.content);
        console.log(`Sent ${job.type} message to ${destination}`);
    }
}

async function consumeOutgoingMessages(): Promise<void> {
    if (!rabbitChannel) {
        throw new Error('RabbitMQ channel is not initialized.');
    }

    await rabbitChannel.consume(OUTGOING_QUEUE_NAME, async (msg: ConsumeMessage | null) => {
        if (!msg) return;

        try {
            const payload = parseJsonPayload(msg.content);
            await sendOutgoingJob(payload);
            rabbitChannel!.ack(msg);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            if (error instanceof ValidationError) {
                console.error(`Discarding invalid outbound WhatsApp job: ${message}`);
                rabbitChannel!.ack(msg);
                return;
            }

            const requeue = error instanceof TransientProcessingError || !msg.fields.redelivered;
            console.error(`Failed to process outbound WhatsApp job. Requeue: ${requeue}. Error:`, error);
            rabbitChannel!.nack(msg, false, requeue);
        }
    });
}

type WrappedMessageContent = {
    ephemeralMessage?: { message?: WAMessage['message'] | null } | null;
    viewOnceMessage?: { message?: WAMessage['message'] | null } | null;
    viewOnceMessageV2?: { message?: WAMessage['message'] | null } | null;
    documentWithCaptionMessage?: { message?: WAMessage['message'] | null } | null;
};

function unwrapMessageContent(content: WAMessage['message']): WAMessage['message'] {
    if (!content) return content;

    const wrapped = content as WrappedMessageContent;
    return wrapped.ephemeralMessage?.message
        ?? wrapped.viewOnceMessage?.message
        ?? wrapped.viewOnceMessageV2?.message
        ?? wrapped.documentWithCaptionMessage?.message
        ?? content;
}

type IncomingMediaSource = {
    url?: string | null;
    directPath?: string | null;
    mediaKey?: Uint8Array | null;
    mimetype?: string | null;
    fileName?: string | null;
    fileLength?: unknown;
    caption?: string | null;
    seconds?: number | null;
    ptt?: boolean | null;
};

async function buildIncomingMediaPayload(
    source: IncomingMediaSource,
    message?: WAMessage,
    downloadBase64 = false,
): Promise<IncomingMediaPayload> {
    const url = source.url ?? undefined;
    const payload: IncomingMediaPayload = {
        url,
        mediaUrl: url,
        directPath: source.directPath ?? undefined,
        mediaKey: bytesToBase64(source.mediaKey),
        mimetype: source.mimetype ?? undefined,
        fileName: source.fileName ?? undefined,
        fileLength: toOptionalNumber(source.fileLength),
        caption: source.caption ?? undefined,
        seconds: source.seconds ?? undefined,
        ptt: source.ptt ?? undefined,
    };

    if (downloadBase64 && message) {
        try {
            const buffer = await downloadMediaMessage(message, 'buffer', {});
            const base64 = Buffer.from(buffer).toString('base64');

            payload.base64 = base64;
            payload.mediaBase64 = base64;

            console.log(
                `Downloaded incoming document "${payload.fileName ?? 'file'}" ` +
                `(${payload.mimetype ?? 'unknown mimetype'}), ${buffer.length} bytes`,
            );
        } catch (error) {
            console.error(`Could not download incoming document "${payload.fileName ?? 'file'}":`, error);
        }
    }

    return payload;
}

function logIncomingDocument(payload: IncomingMediaPayload): void {
    console.log('Incoming document metadata:', JSON.stringify({
        fileName: payload.fileName,
        mimetype: payload.mimetype,
        fileLength: payload.fileLength,
        hasUrl: Boolean(payload.url),
        hasBase64: Boolean(payload.mediaBase64),
    }));
}

async function extractIncomingMessage(message: WAMessage): Promise<IncomingWhatsAppMessage | null> {
    const key = message.key;
    const remoteJid = key?.remoteJid;

    if (!remoteJid || key?.fromMe) {
        return null;
    }

    const content = unwrapMessageContent(message.message);
    if (!content) return null;

    const senderJid = key?.participant ?? remoteJid;
    const group = await getGroupPayload(remoteJid);
    const basePayload = {
        event: 'whatsapp.message.received' as const,
        id: key?.id ?? undefined,
        remoteJid,
        senderJid,
        fromMe: Boolean(key?.fromMe),
        timestamp: toUnixTimestamp(message.messageTimestamp),
        pushName: message.pushName ?? undefined,
        group,
    };

    if (content.imageMessage) {
        return {
            ...basePayload,
            type: 'image',
            text: content.imageMessage.caption ?? undefined,
            media: await buildIncomingMediaPayload(content.imageMessage),
        };
    }

    if (content.audioMessage) {
        return {
            ...basePayload,
            type: 'audio',
            media: await buildIncomingMediaPayload(content.audioMessage),
        };
    }

    if (content.documentMessage) {
        const media = await buildIncomingMediaPayload(
            content.documentMessage,
            message,
            DOWNLOAD_INCOMING_DOCUMENTS,
        );

        logIncomingDocument(media);

        return {
            ...basePayload,
            type: 'document',
            text: content.documentMessage.caption ?? undefined,
            media,
        };
    }

    const text = content.conversation ?? content.extendedTextMessage?.text;
    if (text) {
        return {
            ...basePayload,
            type: 'text',
            text,
        };
    }

    return {
        ...basePayload,
        type: 'unknown',
    };
}

async function connectToWhatsApp(): Promise<void> {
    const { state, saveCreds } = await useMultiFileAuthState(WHATSAPP_AUTH_DIR);
    const version = await getWhatsAppVersion();

    sock = makeWASocket({
        auth: state,
        qrTimeout: 60_000,
        version,
    });

    sock.ev.on('connection.update', (update) => {
        const { connection, qr, lastDisconnect } = update;

        if (qr) {
            console.log('WhatsApp QR code generated. Scan it in WhatsApp > Linked devices.');
            qrcode.generate(qr, { small: true });
        }

        if (connection === 'close') {
            setWhatsAppConnected(false);
            const boom = lastDisconnect?.error as Boom | undefined;
            const error = boom?.output?.statusCode;
            const shouldReconnect = error !== DisconnectReason.loggedOut;

            console.log('WhatsApp connection closed:', error, ', reconnecting:', shouldReconnect);
            if (boom?.data) {
                console.error('WhatsApp disconnect data:', JSON.stringify(boom.data));
            }

            if (shouldReconnect) {
                void connectToWhatsApp();
            } else {
                console.error('WhatsApp logged out. Scan a new QR code after restarting the bot.');
            }
        } else if (connection === 'open') {
            setWhatsAppConnected(true);
            console.log('WhatsApp connection opened successfully.');
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async ({ messages }) => {
        for (const message of messages) {
            try {
                const payload = await extractIncomingMessage(message);
                if (payload) publishIncomingMessage(payload);
            } catch (error) {
                console.error('Error publishing incoming WhatsApp message:', error);
            }
        }
    });
}

async function shutdown(signal: NodeJS.Signals): Promise<void> {
    console.log(`Received ${signal}. Closing RabbitMQ resources.`);

    try {
        await rabbitChannel?.close();
        await rabbitConnection?.close();
    } catch (error) {
        console.error('Error while shutting down:', error);
    } finally {
        process.exit(0);
    }
}

async function start(): Promise<void> {
    await connectRabbitMQ();
    await connectToWhatsApp();
    await consumeOutgoingMessages();
}

process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));

void start().catch((error) => {
    console.error('Error during initialization:', error);
    process.exit(1);
});
