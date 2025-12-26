import uWS from "uWebSockets.js";
import net from "net";

const PORT = 8000;
const app = uWS.App();

// HTTP healthcheck
app.get("/", (res) => {
    res.writeHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("MCP SERVER STATUS: RUNNING!");
});

// WebSocket <-> TCP proxy
app.ws("/*", {
    compression: 0,
    maxPayloadLength: 16 * 1024 * 1024,
    idleTimeout: 300,
    upgrade: (res, req, context) => {
        res.upgrade(
            {
                encoded: req.getUrl().slice(1),
                ip: Buffer.from(res.getRemoteAddressAsText()).toString()
            },
            req.getHeader("sec-websocket-key"),
            req.getHeader("sec-websocket-protocol"),
            req.getHeader("sec-websocket-extensions"),
            context
        );
    },

    open: (ws) => {
        const decoded = Buffer.from(ws.encoded, "base64").toString("utf8");
        const clientIp = ws.ip;
        const [host, portStr] = decoded.split(":");
        const port = parseInt(portStr, 10);

        if (!host || !port) {
            ws.end(1011, "Invalid address");
            return;
        }

        // custom state
        const tcp = net.createConnection({
            host,
            port
        });
        tcp.setTimeout(0);
        tcp.setNoDelay(true);

        ws.isConnected = false;
        ws.queue = [];
        ws.tcp = tcp;

        tcp.on("connect", () => {
            ws.isConnected = true;
            ws.queue.forEach(msg => tcp.write(msg + '\n'));
            ws.queue.length = 0;
            console.log(`🟢 SUCCESS: WS [${clientIp}] <-> TCP [${host}:${port}]`);
        });

        tcp.on("data", (data) => {
            try {
                ws.send(data.toString('utf-8'), false);
            } catch (err) {
                console.error("WS send failed:", err.message);
            }
        });

        tcp.on("close", () => {
            if (ws.isOpen) ws.end(1000, "TCP closed");
        });

        tcp.on("error", (err) => {
            console.error(`TCP error: ${err.message}`);
            if (ws.isOpen) ws.end(1011, err.message);
        });
    },

    tcpSend: (ws, msg) => {
        if (!ws.isConnected) return;
        const text = msg.endsWith("\n") ? msg : msg + "\n";
        ws.tcp.write(text);
    },

    message: (ws, msg) => {
        const data = Buffer.from(msg);
        if (ws.isConnected) {
            ws.tcp.write(data.toString('utf-8') + "\n");
        } else {
            ws.queue.push(data.toString('utf-8'));
        }
    },

    close: (ws) => {
        const clientIp = ws.ip;
        if (ws.tcp && !ws.tcp.destroyed) ws.tcp.destroy();
        console.log(`🔴 DISCONNECTED: WS [${clientIp}]`);
    },
});

app.listen("0.0.0.0", PORT, (t) => {
    if (t) console.log(`🚀 WS⇄TCP proxy running on port ${PORT}`);
    else console.error("❌ Failed to listen");
});
