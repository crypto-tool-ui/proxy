#!/usr/bin/env node
/**
 * WebSocket to TCP Stratum Proxy
 * Dynamic target pool via base64 URL:
 * ws://IP:PORT/base64(host:port)
 */

const WebSocket = require('ws');
const net = require('net');
const http = require('http');

// Configuration
const WS_PORT = process.argv[2] || 8000;

// Create HTTP server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n');
});

// WebSocket server
const wss = new WebSocket.Server({ 
    server,
    perMessageDeflate: false, // Disable compression for performance
    maxPayload: 100 * 1024, // 100KB max message size
    clientTracking: true,
    backlog: 511 // Increase connection queue
});

console.log(`[PROXY] WebSocket listening on port: ${WS_PORT}`);
console.log(`[PROXY] Expected format: ws://IP:PORT/base64(host:port)`);
console.log(`[PROXY] Ready to accept connections...\n`);

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;

    // --- Extract and decode target from URL ---
    const path = req.url?.slice(1); // remove leading "/"
    if (!path) {
        ws.send(JSON.stringify({ error: "Missing base64 target in URL" }));
        ws.close();
        return;
    }

    let decoded, host, port;
    try {
        decoded = Buffer.from(path, 'base64').toString('utf8');
        [host, port] = decoded.split(':');
        if (!host || !port) throw new Error("Invalid target format");
    } catch (err) {
        ws.send(JSON.stringify({ error: "Invalid base64 target" }));
        console.error(`[ERROR] Base64 decode failed:`, err.message);
        ws.close();
        return;
    }

    console.log(`[WS] Connecting from ${clientIp} -> ${host}:${port}`);

    // --- TCP connect to decoded pool ---
    const tcpClient = new net.Socket();

    tcpClient.connect(port, host, () => {
        console.log(`[WS] Connected from ${clientIp} -> ${host}:${port}`);
        // ws.send(JSON.stringify({ status: "connected", target: `${host}:${port}` }));
    });

    // --- WS → TCP ---
    ws.on('message', (data) => {
        try {
            // console.log(`[WS→TCP] ${data}`);
            tcpClient.write(data.toString() + '\n');
        } catch (err) {
            console.error(`[ERROR] WS→TCP failed:`, err.message);
        }
    });

    // --- TCP → WS ---
    tcpClient.on('data', (data) => {
        const msg = data.toString().trim();
        if (msg && ws.readyState === WebSocket.OPEN) {
            // console.log(`[TCP→WS] ${msg}`);
            ws.send(msg);
        }
    });

    // --- Cleanup ---
    ws.on('close', () => {
        // console.log(`[WS] Closed ${clientIp}`);
        tcpClient.end();
    });

    ws.on('error', (err) => {
        console.error(`[WS ERROR]`, err.message);
        tcpClient.end();
    });

    tcpClient.on('close', () => {
        // console.log(`[TCP] Pool socket closed`);
        if (ws.readyState === WebSocket.OPEN) ws.close();
    });

    tcpClient.on('error', (err) => {
        // console.error(`[TCP ERROR]`, err.message);
        if (ws.readyState === WebSocket.OPEN) ws.close();
    });

    tcpClient.setTimeout(300000);
    tcpClient.on('timeout', () => {
        // console.log(`[TCP] Timeout`);
        tcpClient.end();
    });
});

wss.on('error', (err) => console.error(`[WSS ERROR]`, err.message));

// Start server
server.listen(WS_PORT, () => console.log(`[SERVER] Listening on port ${WS_PORT}`));
