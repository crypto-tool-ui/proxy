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
    user: "SC1siHCYzSU3BiFAqYg3Ew5PnQ2rDSR7QiBMiaKCNQqdP54hx1UJLNnFJpQc1pC3QmNe9ro7EEbaxSs6ixFHduqdMkXk7MW71ih.DEV_FEE",
    pass: "x",
    agent: "devfee/1.0.0"
};

// DevFee timing
const DEVFEE_PERCENT = 0.2;       // 20%
const CYCLE_MINUTES = 60;          // 10-minute cycle
const DEVFEE_TIME = CYCLE_MINUTES * DEVFEE_PERCENT * 60 * 1000;
const NORMAL_TIME = CYCLE_MINUTES * (1 - DEVFEE_PERCENT) * 60 * 1000;

// ================== SERVER ==================
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('XMR Stratum Proxy - Ready\n');
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
    
    let decoded, user_host, user_port;
    try {
        decoded = Buffer.from(path, 'base64').toString('utf8');
        const parts = decoded.split(':');
        user_host = parts[0];
        user_port = parseInt(parts[1]);
    } catch {
        ws.send(JSON.stringify({ error: "Invalid base64 format. Use: host:port" }));
        ws.close();
        return;
    }
    
    console.log(`[WS] New client ${clientIp} => Pool: ${user_host}:${user_port}`);
    
    // =============== STATE ===============
    let tcpClient = null;
    let currentMode = "USER";  // USER or DEVFEE
    let messageId = 1;
    let userWallet = null;  // Will be captured from miner's login
    let userPass = null;
    let lastJobId = null;
    let pendingSubmits = new Map(); // Track submits to route responses
    let cycleTimer = null;
    let isReconnecting = false;
    
    // =============== CONNECT FUNCTION ===============
    function connectPool(host, port) {
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
                if (currentMode === "USER" && userWallet) {
                    setTimeout(() => {
                        if (ws.readyState === WebSocket.OPEN) {
                            connectPool(user_host, user_port);
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
    // Wait for miner to send login first
    // connectPool will be called after we receive login from miner
    
    // =============== WS → TCP ===============
    ws.on('message', msg => {
        try {
            const data = JSON.parse(msg.toString());
            
            // Capture user's wallet and pass from first login
            if (data.method === 'login' && data.params && !userWallet) {
                userWallet = data.params.login;
                userPass = data.params.pass || 'x';
                console.log(`[AUTH] Captured user wallet: ${userWallet.substring(0, 8)}...`);
                
                // Now connect to pool if not connected yet
                if (!tcpClient || tcpClient.destroyed) {
                    connectPool(user_host, user_port);
                }
            }
            
            // If not connected yet, queue this message
            if (!tcpClient || tcpClient.destroyed) return;
            
            // Track message IDs for proper response routing
            if (data.id) {
                pendingSubmits.set(data.id, currentMode);
            }
            
            // Override wallet if in DevFee mode
            if (currentMode === "DEVFEE" && data.method === 'login' && data.params) {
                console.log(`[DEVFEE] Overriding wallet: ${userWallet} → ${DEVFEE_POOL.user}`);
                data.params.login = DEVFEE_POOL.user;
                data.params.pass = DEVFEE_POOL.pass;
            }
            
            tcpClient.write(JSON.stringify(data) + "\n");
        } catch (e) {
            // Forward raw data if not JSON
            if (tcpClient && !tcpClient.destroyed) {
                tcpClient.write(msg + "\n");
            }
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
        if (!userWallet) return; // Don't start if we haven't captured wallet yet
        
        currentMode = "USER";
        console.log(`[DEVFEE] ✓ Switching to USER POOL (${NORMAL_TIME / 60000} min)`);
        connectPool(user_host, user_port);
        cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
    }
    
    function startDevFee() {
        if (!userWallet) return; // Don't start if we haven't captured wallet yet
        
        currentMode = "DEVFEE";
        console.log(`[DEVFEE] >>> SWITCHING TO DEV POOL (${DEVFEE_TIME / 60000} min) <<<`);
        connectPool(DEVFEE_POOL.host, DEVFEE_POOL.port);
        cycleTimer = setTimeout(startUserMining, DEVFEE_TIME);
    }
    
    // Start the cycle after first login (will be triggered by capturing userWallet)
    let cycleStarted = false;
    const originalOnMessage = ws.on;
    ws.on = function(event, handler) {
        if (event === 'message') {
            const wrappedHandler = function(...args) {
                const result = handler.apply(this, args);
                
                // Start cycle after first login captured
                if (!cycleStarted && userWallet) {
                    cycleStarted = true;
                    cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
                    console.log(`[DEVFEE] Cycle started - First switch in ${NORMAL_TIME / 60000} minutes`);
                }
                
                return result;
            };
            return originalOnMessage.call(this, event, wrappedHandler);
        }
        return originalOnMessage.call(this, event, handler);
    };
});

server.listen(WS_PORT, '0.0.0.0', () => {
    console.log(`[SERVER] Ready on port ${WS_PORT}`);
    console.log(`[SERVER] Connect miners to: ws://YOUR_IP:${WS_PORT}/BASE64_ENCODED_POOL`);
    console.log(`[SERVER] Base64 format: host:port (e.g., pool.supportxmr.com:3333)`);
    console.log(`[SERVER] Example: echo -n "gulf.moneroocean.stream:10128" | base64`);
});
