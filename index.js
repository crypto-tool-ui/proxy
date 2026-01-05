import uWS from "uWebSockets.js";
import net from "net";

const PORT = process.env.PORT || 8080;
const app = uWS.App();

// Cấu hình tối ưu cho mining
const MAX_ENCODED_LENGTH = 1024;
const MAX_PENDING_MESSAGES = 100;      // Giảm xuống, mining thường ít message ban đầu
const MAX_PAYLOAD_LENGTH = 16 * 1024;  // 16KB đủ cho mining JSON-RPC
const IDLE_TIMEOUT_SECONDS = 600;      // 10 phút cho mining
const TCP_CONNECT_TIMEOUT = 15000;     // 15s timeout để connect TCP
const DEBUG = process.env.DEBUG === "true";

// Stats tracking
const stats = {
    activeConnections: 0,
    totalConnections: 0,
    errors: { overflow: 0, tcpTimeout: 0, tcpError: 0 }
};

function normalizeLine(msg) {
    const text = typeof msg === "string" ? msg : String(msg);
    return text.endsWith("\n") ? text : text + "\n";
}

function safeEndWS(ws, code, reason) {
    try {
        if (ws.isClosed) return;
        if (ws.isOpen) {
            ws.end(code, reason);
        }
    } catch (e) {
        if (DEBUG) console.error("Error closing WebSocket:", e);
    }
}

function isForbiddenHost(host) {
    const lower = host.toLowerCase();
    const allows = process.env.ALLOW_HOSTS || "";
    
    if (allows) {
        const list = allows.split(",").map(h => h.trim().toLowerCase());
        if (list.includes(lower)) return false;
    }
    
    // Chặn localhost/internal nếu cần
    // return lower === "127.0.0.1" || lower === "localhost";
    return false;
}

// Cleanup function
function cleanupConnection(ws) {
    if (ws.tcp && !ws.tcp.destroyed) {
        try {
            ws.tcp.destroy();
        } catch (e) {
            if (DEBUG) console.error("Error destroying TCP:", e);
        }
    }
    
    if (ws.connectTimeout) {
        clearTimeout(ws.connectTimeout);
        ws.connectTimeout = null;
    }
    
    ws.isConnected = false;
    ws.pendingMessages = [];
    ws.tcp = null;
    ws.paused = false;
    
    stats.activeConnections--;
}

// HTTP healthcheck với stats
app.get("/", (res) => {
    res.writeHeader("Content-Type", "application/json; charset=utf-8");
    res.writeHeader("Cache-Control", "no-store");
    res.end(JSON.stringify({
        status: "running",
        stats: {
            active: stats.activeConnections,
            total: stats.totalConnections,
            errors: stats.errors
        }
    }, null, 2));
});

// WebSocket <-> TCP proxy
app.ws("/*", {
    compression: 0,
    maxPayloadLength: MAX_PAYLOAD_LENGTH,
    idleTimeout: IDLE_TIMEOUT_SECONDS,
    maxBackpressure: 64 * 1024, // 64KB backpressure buffer
    
    upgrade: (res, req, context) => {
        try {
            const encoded = req.getUrl().slice(1);
            if (!encoded || encoded.length > MAX_ENCODED_LENGTH) {
                res.writeStatus("400 Bad Request").end("Invalid encoded length");
                return;
            }
            
            const ip = Buffer.from(res.getRemoteAddressAsText()).toString();
            
            res.upgrade(
                {
                    encoded,
                    ip,
                    isConnected: false,
                    pendingMessages: [],
                    tcp: null,
                    tcpHost: null,
                    connectTimeout: null,
                    paused: false
                },
                req.getHeader("sec-websocket-key"),
                req.getHeader("sec-websocket-protocol"),
                req.getHeader("sec-websocket-extensions"),
                context
            );
        } catch (e) {
            console.error("Upgrade error:", e);
            try {
                res.writeStatus("500 Internal Server Error").end("Upgrade failed");
            } catch {}
        }
    },
    
    open: (ws) => {
        stats.totalConnections++;
        stats.activeConnections++;
        
        let decoded;
        try {
            if (typeof ws.encoded !== "string" || ws.encoded.length === 0) {
                safeEndWS(ws, 1011, "Missing encoded address");
                return;
            }
            decoded = Buffer.from(ws.encoded, "base64").toString("utf8");
        } catch {
            safeEndWS(ws, 1011, "Invalid base64");
            return;
        }
        
        const [host, portStr] = decoded.split(":");
        const port = Number.parseInt(portStr || "", 10);
        
        if (!host || !Number.isInteger(port) || port < 1 || port > 65535) {
            safeEndWS(ws, 1011, "Invalid address");
            return;
        }
        
        if (isForbiddenHost(host)) {
            safeEndWS(ws, 1011, "Forbidden target");
            return;
        }
        
        const clientIp = ws.ip;
        ws.tcpHost = `${host}:${port}`;
        ws.pendingMessages = [];
        
        if (DEBUG) {
            console.log(`🔵 Connecting WS [${clientIp}] -> TCP [${host}:${port}]`);
        }
        
        // Tạo TCP connection
        const tcp = net.createConnection({ host, port });
        tcp.setNoDelay(true);
        tcp.setKeepAlive(true, 60000); // Keep-alive cho mining
        
        ws.tcp = tcp;
        ws.isConnected = false;
        
        // Timeout nếu không connect được
        ws.connectTimeout = setTimeout(() => {
            if (!ws.isConnected) {
                console.error(`⏱️ TCP timeout [${host}:${port}] for WS [${clientIp}]`);
                stats.errors.tcpTimeout++;
                safeEndWS(ws, 1011, "TCP connection timeout");
                tcp.destroy();
            }
        }, TCP_CONNECT_TIMEOUT);
        
        // TCP connected
        tcp.on("connect", () => {
            clearTimeout(ws.connectTimeout);
            ws.connectTimeout = null;
            ws.isConnected = true;
            
            // Flush pending messages với backpressure handling
            if (ws.pendingMessages.length > 0) {
                if (DEBUG) {
                    console.log(`📤 Flushing ${ws.pendingMessages.length} pending messages`);
                }
                
                for (const msg of ws.pendingMessages) {
                    const canWrite = tcp.write(normalizeLine(msg));
                    if (!canWrite) {
                        // Buffer đầy, đợi drain
                        tcp.once('drain', () => {
                            // Ghi tiếp các message còn lại
                            const remaining = ws.pendingMessages.splice(0);
                            for (const m of remaining) {
                                tcp.write(normalizeLine(m));
                            }
                        });
                        break;
                    }
                }
                ws.pendingMessages = [];
            }
            
            console.log(`🟢 SUCCESS: WS [${clientIp}] <-> TCP [${host}:${port}]`);
        });
        
        // TCP data -> WebSocket
        tcp.on("data", (data) => {
            try {
                if (ws.isClosed) return;
                
                // Kiểm tra backpressure trước khi send
                const backpressure = ws.getBufferedAmount();
                if (backpressure > MAX_PAYLOAD_LENGTH) {
                    if (DEBUG) {
                        console.warn(`⚠️ WS backpressure high: ${backpressure} bytes`);
                    }
                    // Tạm dừng TCP để tránh overflow
                    if (!ws.paused) {
                        tcp.pause();
                        ws.paused = true;
                    }
                    return;
                }
                
                ws.send(data, false);
                
                // Resume TCP nếu đã pause
                if (ws.paused && backpressure < MAX_PAYLOAD_LENGTH / 2) {
                    tcp.resume();
                    ws.paused = false;
                }
            } catch (err) {
                console.error("WS send failed:", err?.message ?? err);
            }
        });
        
        // TCP closed
        tcp.on("close", () => {
            if (DEBUG) {
                console.log(`🔴 TCP closed [${host}:${port}] for WS [${clientIp}]`);
            }
            cleanupConnection(ws);
            safeEndWS(ws, 1000, "TCP closed");
        });
        
        // TCP error
        tcp.on("error", (err) => {
            console.error(`❌ TCP error [${host}:${port}] for WS [${clientIp}]:`, err.message);
            stats.errors.tcpError++;
            cleanupConnection(ws);
            safeEndWS(ws, 1011, err.message || "TCP error");
        });
    },
    
    // WebSocket message -> TCP
    message: (ws, msg) => {
        try {
            const data = Buffer.from(msg);
            const text = data.toString("utf-8");
            
            // Nếu TCP chưa connect
            if (!ws.isConnected) {
                if (ws.pendingMessages.length >= MAX_PENDING_MESSAGES) {
                    console.error(`🚨 Pending queue overflow for WS [${ws.ip}]`);
                    stats.errors.overflow++;
                    safeEndWS(ws, 1011, "Too many pending messages");
                    return;
                }
                
                ws.pendingMessages.push(text);
                
                if (DEBUG) {
                    console.log(`📥 Queued message (${ws.pendingMessages.length}/${MAX_PENDING_MESSAGES})`);
                }
                return;
            }
            
            // TCP đã connect, gửi trực tiếp
            if (ws.tcp && !ws.tcp.destroyed) {
                const canWrite = ws.tcp.write(normalizeLine(text));
                
                // Nếu TCP buffer đầy
                if (!canWrite) {
                    if (DEBUG) {
                        console.warn(`⚠️ TCP backpressure for [${ws.tcpHost}]`);
                    }
                    
                    // Đợi drain event
                    ws.tcp.once('drain', () => {
                        if (DEBUG) {
                            console.log(`✅ TCP drained for [${ws.tcpHost}]`);
                        }
                    });
                }
            }
        } catch (err) {
            console.error("Message handling error:", err);
        }
    },
    
    // WebSocket closed
    close: (ws, code, message) => {
        const reason = Buffer.from(message || "").toString("utf-8") || "no reason";
        
        console.log(
            `🔴 DISCONNECTED: WS [${ws.ip}] <-> TCP [${ws.tcpHost}] (code=${code}, reason="${reason}")`
        );
        
        cleanupConnection(ws);
    },
    
    // WebSocket drain event (khi buffer trống)
    drain: (ws) => {
        if (ws.paused && ws.tcp && !ws.tcp.destroyed) {
            ws.tcp.resume();
            ws.paused = false;
            if (DEBUG) {
                console.log(`✅ WS drained, resumed TCP for [${ws.tcpHost}]`);
            }
        }
    }
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down gracefully...');
    console.log(`Final stats: ${JSON.stringify(stats, null, 2)}`);
    process.exit(0);
});

app.listen("0.0.0.0", PORT, (token) => {
    if (token) {
        console.log(`🚀 Mining Proxy running on port ${PORT}`);
        console.log(`📊 Health check: http://localhost:${PORT}/`);
    } else {
        console.error("❌ Failed to listen on port", PORT);
        process.exit(1);
    }
});
