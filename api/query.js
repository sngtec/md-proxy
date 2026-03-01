// api/query.js
import duckdb from 'duckdb';
import fs from 'fs';

// ==========================================
// GLOBAL SCOPE (Runs once per container boot)
// ==========================================

// 1. Ensure the extension directory exists once
const extDir = '/tmp/duckdb_extensions';
if (!fs.existsSync(extDir)) {
  fs.mkdirSync(extDir, { recursive: true });
}

// 2. Create a cache to hold active database connections
const connectionCache = new Map();

// ==========================================
// REQUEST HANDLER (Runs on every API call)
// ==========================================
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  // --- NEW: SECURITY CHECK ---
  // Ensure the caller provided the correct Proxy API Key
  const providedKey = req.headers['x-proxy-api-key'];
  const expectedKey = process.env.PROXY_API_KEY;

  if (!expectedKey) {
    return res.status(500).json({ error: 'Server misconfiguration: PROXY_API_KEY is not set.' });
  }
  if (providedKey !== expectedKey) {
    return res.status(403).json({ error: 'Forbidden: Invalid or missing Proxy API Key.' });
  }
  // ---------------------------

  const { sql } = req.body;
  if (!sql) {
    return res.status(400).json({ error: 'Missing "sql" property in request body.' });
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header.' });
  }
  const token = authHeader.split(' ')[1];

  try {
    // --- NEW: CONNECTION CACHING ---
    let db = connectionCache.get(token);

    // If we don't have an active connection for this token, create and cache it
    if (!db) {
      db = await new Promise((resolve, reject) => {
        const config = { 'extension_directory': extDir };
        const database = new duckdb.Database(`md:?motherduck_token=${token}`, config, (err) => {
          if (err) reject(new Error(`MotherDuck Auth/Connection Failed: ${err.message}`));
          else resolve(database);
        });
      });
      
      connectionCache.set(token, db);
    }
    // -------------------------------

    // Execute the query
    const results = await new Promise((resolve, reject) => {
      db.all(sql, (err, rows) => {
        if (err) reject(new Error(`Query Error: ${err.message}`));
        else resolve(rows);
      });
    });

    // Safely stringify the results to handle BigInts
    const safeJson = JSON.stringify({ data: results }, (key, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );

    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(safeJson);

  } catch (error) {
    console.error('Execution error:', error);
    return res.status(500).json({ error: error.message });
  } 
  // Notice we removed the 'finally' block that closed the DB!
  // Vercel will naturally clean up memory when the container is spun down after a period of inactivity.
}