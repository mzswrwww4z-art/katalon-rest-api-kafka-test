'use strict';

const http = require('node:http');
const { randomUUID } = require('node:crypto');

const records = new Map();
const maxBodyBytes = 32 * 1024;

function sendJson(response, statusCode, value) {
  const body = JSON.stringify(value);
  response.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    'cache-control': 'no-store'
  });
  response.end(body);
}

function readJson(request) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    request.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBodyBytes) {
        reject(Object.assign(new Error('Request body is too large.'), { statusCode: 413 }));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(Object.assign(new Error('Request body must be valid JSON.'), { statusCode: 400 }));
      }
    });
    request.on('error', reject);
  });
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://127.0.0.1');

  if (request.method === 'GET' && url.pathname === '/health') {
    return sendJson(response, 200, { status: 'ok' });
  }

  if (request.method === 'POST' && url.pathname === '/api/messages') {
    try {
      const body = await readJson(request);
      if (!body || typeof body !== 'object' || Array.isArray(body) || !body.eventId || !body.eventType) {
        return sendJson(response, 400, { error: 'eventId and eventType are required.' });
      }

      const record = {
        id: randomUUID(),
        eventId: String(body.eventId),
        eventType: String(body.eventType),
        payload: body.payload ?? null,
        createdAt: new Date().toISOString()
      };
      records.set(record.id, record);
      return sendJson(response, 201, record);
    } catch (error) {
      return sendJson(response, error.statusCode || 400, { error: error.message || 'Invalid request.' });
    }
  }

  const messageMatch = url.pathname.match(/^\/api\/messages\/([^/]+)$/);
  if (request.method === 'GET' && messageMatch) {
    const record = records.get(decodeURIComponent(messageMatch[1]));
    if (!record) {
      return sendJson(response, 404, { error: 'Message not found.' });
    }
    return sendJson(response, 200, record);
  }

  return sendJson(response, 404, { error: 'Route not found.' });
});

const port = Number(process.env.PORT || 8080);
server.listen(port, '0.0.0.0', () => {
  console.log(`Local demo REST API listening on port ${port}`);
});
