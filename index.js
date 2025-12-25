#!/usr/bin/env node
/**
 * WebSocket to TCP Stratum Proxy (uWebSockets.js)
 * Dynamic target pool via base64 URL:
 * ws://IP:PORT/base64(host:port)
 */
import uWS from 'uWebSockets.js';
import net from 'net';
import dns from 'dns/promises';
import { StringDecoder } from 'string_decoder';

const WS_PORT = Number(process.argv[2] || 8000);

console.log(`[PROXY] WebSocket listening on port: ${WS_PORT}`);
console.log(`[PROXY] Expected format: ws://IP:PORT/base64(host:port)`);
console.log(`[PROXY] Ready to accept connections...`);

uWS.App()
  /* ---------- HTTP fallback ---------- */
  .any('/*', (res, req) => {
    res.writeHeader('Content-Type', 'text/plain');
    res.end('WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n');
  })
  
  /* ---------- WebSocket ---------- */
  .ws('/*', {
    compression: uWS.DISABLED,
    maxPayloadLength: 100 * 1024,
    idleTimeout: 300,
    sendPingsAutomatically: false,
    
    async open(ws) {
      const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
      
      // ---- decode base64 target ----
      const path = ws.path?.slice(1);
      if (!path) {
        ws.send(JSON.stringify({ error: 'Missing base64 target in URL' }), false);
        ws.close();
        return;
      }
      
      let host, port;
      try {
        const decoded = Buffer.from(path, 'base64').toString('utf8');
        [host, port] = decoded.split(':');
        if (!host || !port) throw new Error('Invalid target');
      } catch (e) {
        ws.send(JSON.stringify({ error: 'Invalid base64 target' }), false);
        ws.close();
        return;
      }
      
      // ✅ DNS Resolution
      let resolvedIp;
      try {
        const addresses = await dns.resolve4(host);
        resolvedIp = addresses[0];
      } catch (err) {
        console.error(`[DNS ERROR] Failed to resolve ${host}:`, err.message);
        ws.send(JSON.stringify({ error: `DNS resolution failed for ${host}` }), false);
        ws.close();
        return;
      }
      
      console.log(`[WS] Connecting from ${clientIp} -> ${host} (${resolvedIp}):${port}`);
      
      // ---- TCP socket ----
      const tcpClient = new net.Socket();
      tcpClient.setNoDelay(true); // ✅ Disable Nagle's algorithm
      tcpClient.setKeepAlive(true, 60000);
      
      // ✅ StringDecoder for proper UTF-8 handling
      const decoder = new StringDecoder('utf8');
      
      // ✅ Line buffer for Stratum protocol
      let lineBuffer = '';
      let isConnected = false;
      
      // Store references
      ws.tcpClient = tcpClient;
      ws.decoder = decoder;
      ws.lineBuffer = lineBuffer;
      ws.isConnected = false;
      
      tcpClient.connect(Number(port), resolvedIp, () => {
        ws.isConnected = true;
        console.log(`[TCP] Connected from ${clientIp} -> ${host} (${resolvedIp}):${port}`);
        
        // ✅ Send initial TEXT message
        if (!ws.closed) {
          ws.send(JSON.stringify({ 
            type: 'connected', 
            target: `${host}:${port}`,
            ip: resolvedIp
          }), false, false); // isBinary=false, compress=false
        }
      });
      
      // TCP → WS (✅ FIX: Proper UTF-8 handling)
      tcpClient.on('data', (data) => {
        if (ws.closed) return;
        
        try {
          // ✅ Decode with StringDecoder (handles incomplete UTF-8)
          const text = ws.decoder.write(data);
          
          if (text) {
            // ✅ Line buffering for Stratum (JSON-RPC)
            ws.lineBuffer += text;
            const lines = ws.lineBuffer.split('\n');
            ws.lineBuffer = lines.pop() || ''; // Keep incomplete line
            
            // ✅ Send each complete line as TEXT
            lines.forEach(line => {
              if (line.trim() && !ws.closed) {
                ws.send(line, false, false); // TEXT mode, no compression
              }
            });
          }
        } catch (err) {
          console.error(`[ERROR] TCP→WS:`, err.message);
        }
      });
      
      tcpClient.on('close', () => {
        // ✅ Flush remaining data
        try {
          if (!ws.closed) {
            const remaining = ws.decoder.end();
            if (remaining) {
              ws.lineBuffer += remaining;
            }
            if (ws.lineBuffer.trim()) {
              ws.send(ws.lineBuffer, false, false);
            }
          }
        } catch (err) {
          console.error(`[ERROR] Flush on close:`, err.message);
        }
        
        ws.isConnected = false;
        if (!ws.closed) ws.close();
      });
      
      tcpClient.on('error', (err) => {
        console.error(`[TCP ERROR] ${host} (${resolvedIp}):${port}:`, err.message);
        ws.isConnected = false;
        if (!ws.closed) ws.close();
      });
      
      tcpClient.setTimeout(300000, () => {
        console.log(`[TCP] Timeout for ${host}:${port}`);
        tcpClient.end();
      });
    },
    
    message(ws, message, isBinary) {
      // WS → TCP
      if (!ws.isConnected) return;
      
      try {
        const data = Buffer.from(message);
        ws.tcpClient?.write(data);
        ws.tcpClient?.write('\n');
      } catch (err) {
        console.error(`[ERROR] WS→TCP:`, err.message);
      }
    },
    
    close(ws, code, message) {
      ws.isConnected = false;
      ws.tcpClient?.end();
    }
  })
  
  .listen(WS_PORT, (token) => {
    if (token) {
      console.log(`[SERVER] Listening on port ${WS_PORT} (uWebSockets.js)`);
    } else {
      console.error('[SERVER] Failed to listen');
      process.exit(1);
    }
  });

// ✅ Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[SERVER] Shutting down...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('[SERVER] Shutting down...');
  process.exit(0);
});
