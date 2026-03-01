// api/query.js
import duckdb from 'duckdb';
import fs from 'fs';
import { LRUCache } from 'lru-cache';

// 1. Ensure the extension directory exists once
const extDir = '/tmp/duckdb_extensions';
if (!fs.existsSync(extDir)) {
  fs.mkdirSync(extDir, { recursive: true });
}

// 2. Set up the LRU Cache for database connections
const connectionCache = new LRUCache({
  max: 10, // Max number of concurrent client connections to keep warm
  ttl: 1000 * 60 * 30, // 30 minutes. If a connection sits idle, evict it.
  
  // THE MAGIC: This runs automatically whenever a connection is evicted from the cache!
  dispose: (db, token) => {
    // We only log the first 8 characters of the token for security!
    const maskedToken = token.substring(0, 8) + '...';
    console.log(`[Cache Cleanup] Closing connection for token ${maskedToken}. Reason: ${reason}`);
    db.close(); 
  }
});

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  // Security Check
  const providedKey = req.headers['x-proxy-api-key'];
  const expectedKey = process.env.PROXY_API_KEY;
  if (!expectedKey) return res.status(500).json({ error: 'Server misconfiguration.' });
  if (providedKey !== expectedKey) return res.status(403).json({ error: 'Forbidden.' });

  const { sql } = req.body;
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header.' });
  }
  const token = authHeader.split(' ')[1];

  try {
    // 3. Check our smart LRU Cache
    let db = connectionCache.get(token);

    if (!db) {
      db = await new Promise((resolve, reject) => {
        const config = { 'extension_directory': extDir };
        const database = new duckdb.Database(`md:?motherduck_token=${token}`, config, (err) => {
          if (err) reject(new Error(`MotherDuck Auth Failed: ${err.message}`));
          else resolve(database);
        });
      });
      
      // Save it to the cache. If this makes it 11 connections, the oldest is disposed!
      connectionCache.set(token, db);
    }

    const results = await new Promise((resolve, reject) => {
      db.all(sql, (err, rows) => {
        if (err) reject(new Error(`Query Error: ${err.message}`));
        else resolve(rows);
      });
    });

    const safeJson = JSON.stringify({ data: results }, (key, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );

    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(safeJson);

  } catch (error) {
    console.error('Execution error:', error);
    return res.status(500).json({ error: error.message });
  }
}