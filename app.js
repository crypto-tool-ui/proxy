#!/usr/bin/env node
/**
 * WebSocket to TCP Stratum Proxy with DNS Resolution
 * Dynamic target pool via base64 URL:
 * ws://IP:PORT/base64(host:port)
 */
const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const dns = require('dns').promises;

// Configuration
const WS_PORT = process.env.PORT || 8080;

// Create HTTP server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('MCP SERVER READY !!! \n');
});

// WebSocket server
const wss = new WebSocket.Server({ 
    server,
    perMessageDeflate: false, // Disable compression for performance
    maxPayload: 100 * 1024, // 100KB max message size
});

console.log(`[PROXY] WebSocket listening on port: ${WS_PORT}`);

function validatePort(port) {
    const p = parseInt(port);
    return p > 0 && p <= 65535;
}

wss.on('connection', async (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    
    // --- Extract and decode target from URL ---
    const path = req.url?.slice(1); // remove leading "/"

    if (!path) {
        console.error(`[ERROR] No path provided from ${clientIp}`);
        ws.close();
        return;
    }
    
    let decoded, host, port;
    try {
        decoded = Buffer.from(path, 'base64').toString('utf8');
        [host, port] = decoded.split(':');
        if (!host || !port) throw new Error("Invalid target format");
        if (!validatePort(port)) throw new Error("Invalid port number");
        port = parseInt(port);
    } catch (err) {
        console.error(`[ERROR] Base64 decode failed:`, err.message);
        ws.close();
        return;
    }
    
    // --- DNS Lookup to get IP address | GibhQ-00 ---
    let isClosing = false;
    
    console.log(`[WS] Connecting from ${clientIp} -> ${host}:${port}`);
    
    // --- TCP connect to resolved IP ---
    const tcpClient = new net.Socket();
    
    // Cleanup function
    const cleanup = () => {
        if (isClosing) return;
        isClosing = true;
        
        // Remove all listeners để tránh memory leak
        ws.removeAllListeners();
        tcpClient.removeAllListeners();
        
        // Đóng connections
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
            ws.close();
        }
        
        if (!tcpClient.destroyed) {
            tcpClient.destroy(); // destroy() mạnh hơn end()
        }
    };
    
    tcpClient.connect(port, host, () => {
        console.log(`[TCP] Connected from ${clientIp} -> ${host}:${port}`);
    });
    tcpClient.setNoDelay(true);
    tcpClient.setKeepAlive(true, 30000);
    tcpClient.setTimeout(0);
    
    // WS → TCP
    ws.on('message', (data) => {
        if (isClosing || tcpClient.destroyed) return;
        try {
            const msg = data.toString('utf-8');
            const message = msg.endsWith("\n") ? msg : msg + "\n";
            tcpClient.write(message);
        } catch (err) {
            console.error(`[ERROR] WS→TCP failed:`, err.message);
            cleanup();
        }
    });
    
    // TCP → WS
    tcpClient.on('data', (data) => {
        if (isClosing || ws.readyState !== WebSocket.OPEN) return;
        try {
            const text = data.toString('utf-8');
            ws.send(text, { binary: false });
        } catch (err) {
            console.error(`[ERROR] TCP→WS:`, err.message);
            cleanup();
        }
    });
    
    // Cleanup events
    ws.on('close', cleanup);
    ws.on('error', (err) => {
        console.error(`[WS ERROR]`, err.message);
        cleanup();
    });
    
    tcpClient.on('close', () => {
        console.log(`[TCP] Pool socket closed for ${host}:${port}`);
        cleanup();
    });
    
    tcpClient.on('error', (err) => {
        console.error(`[TCP ERROR] ${host}:${port}:`, err.message);
        cleanup();
    });
    
    tcpClient.on('timeout', () => {
        console.log(`[TCP] Timeout for ${host}:${port}`);
        cleanup();
    });
});

wss.on('error', (err) => console.error(`[WSS ERROR]`, err.message));

// Start server
server.listen(WS_PORT, () => console.log(`[SERVER] Listening on port ${WS_PORT}`));
