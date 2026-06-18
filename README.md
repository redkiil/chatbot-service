# WhatsApp Bot Service

This service connects to WhatsApp through Baileys and RabbitMQ.

- It consumes outbound WhatsApp send jobs from `WHATSAPP_OUTGOING_QUEUE`.
- It publishes incoming WhatsApp messages to `WHATSAPP_INCOMING_EXCHANGE`.
- It also declares `WHATSAPP_INCOMING_QUEUE` and binds it to the incoming routes so one or more worker services can consume received messages.

## Configuration

Copy `.env.example` to `.env` and adjust the RabbitMQ URL or queue names when needed.

The default outbound queue remains `WhatsComs` for compatibility with the previous project.

## Outbound Queue Contract

Publish JSON to the outbound queue:

```json
{
  "destinations": ["5511999999999@s.whatsapp.net"],
  "message": {
    "type": "text",
    "text": "Hello from another service"
  }
}
```

Single destination fields are also accepted: `jid`, `remoteJid`, or `destination`.

Image message:

```json
{
  "jid": "5511999999999@s.whatsapp.net",
  "message": {
    "type": "image",
    "mediaUrl": "https://example.com/image.jpg",
    "caption": "Optional caption"
  }
}
```

Audio message:

```json
{
  "jid": "5511999999999@s.whatsapp.net",
  "message": {
    "type": "audio",
    "mediaUrl": "https://example.com/audio.mp3",
    "mimetype": "audio/mpeg",
    "ptt": false
  }
}
```

Document or ZIP message:

```json
{
  "jid": "5511999999999@s.whatsapp.net",
  "message": {
    "type": "document",
    "mediaBase64": "UEsDB...",
    "mimetype": "application/zip",
    "fileName": "report.zip",
    "caption": "Optional caption"
  }
}
```

Media can be sent with `mediaUrl`/`url` or `mediaBase64`/`base64`.

## Incoming Message Contract

Incoming WhatsApp messages are published with routing keys:

- `whatsapp.incoming.text`
- `whatsapp.incoming.image`
- `whatsapp.incoming.audio`
- `whatsapp.incoming.document`
- `whatsapp.incoming.location`
- `whatsapp.incoming.unknown`

Example payload:

```json
{
  "event": "whatsapp.message.received",
  "id": "MESSAGE_ID",
  "remoteJid": "5511999999999@s.whatsapp.net",
  "senderJid": "5511999999999@s.whatsapp.net",
  "fromMe": false,
  "type": "text",
  "text": "hello",
  "group": {
    "jid": "120363000000000000@g.us",
    "subject": "Group name",
    "participantCount": 10
  },
  "timestamp": 1710000000,
  "pushName": "Contact name"
}
```

The `group` object is present only when `remoteJid` is a group JID ending with `@g.us`. In group messages, `remoteJid` is the group and `senderJid` is the person who sent the message.

Incoming document/ZIP payload:

```json
{
  "event": "whatsapp.message.received",
  "remoteJid": "5511999999999@s.whatsapp.net",
  "senderJid": "5511999999999@s.whatsapp.net",
  "fromMe": false,
  "type": "document",
  "media": {
    "url": "https://mmg.whatsapp.net/...",
    "mediaUrl": "https://mmg.whatsapp.net/...",
    "mimetype": "application/zip",
    "fileName": "report.zip",
    "fileLength": 12345,
    "base64": "UEsDB...",
    "mediaBase64": "UEsDB..."
  }
}
```

Set `WHATSAPP_DOWNLOAD_INCOMING_DOCUMENTS=false` to publish document metadata without embedding the base64 file in RabbitMQ.

For media messages, the payload includes a `media` object with WhatsApp media metadata such as `url`, `directPath`, `mediaKey`, `mimetype`, and `fileLength`.

Incoming location payload:

```json
{
  "event": "whatsapp.message.received",
  "remoteJid": "5511999999999@s.whatsapp.net",
  "senderJid": "5511999999999@s.whatsapp.net",
  "fromMe": false,
  "type": "location",
  "location": {
    "latitude": -15.6014,
    "longitude": -56.0979,
    "name": "Dropped pin",
    "address": "Cuiabá, MT",
    "url": "https://maps.google.com/?q=-15.6014,-56.0979"
  }
}
```
