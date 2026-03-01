// api/query.js
import duckdb from 'duckdb';
import fs from 'fs';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  const { sql } = req.body;
  if (!sql) {
    return res.status(400).json({ error: 'Missing "sql" property in request body.' });
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header.' });
  }
  const token = authHeader.split(' ')[1];

  let db;
  try {
    // 1. CRITICAL: Create a writable directory in Vercel's /tmp folder for DuckDB extensions
    const extDir = '/tmp/duckdb_extensions';
    if (!fs.existsSync(extDir)) {
      fs.mkdirSync(extDir, { recursive: true });
    }

    // 2. Initialize connection, explicitly passing the new extension directory
    db = await new Promise((resolve, reject) => {
      const config = {
        'extension_directory': extDir
      };
      
      const database = new duckdb.Database(`md:?motherduck_token=${token}`, config, (err) => {
        if (err) {
          reject(new Error(`MotherDuck Auth/Connection Failed: ${err.message}`));
        } else {
          resolve(database);
        }
      });
    });

    // 3. Execute the query
    const results = await new Promise((resolve, reject) => {
      db.all(sql, (err, rows) => {
        if (err) reject(new Error(`Query Error: ${err.message}`));
        else resolve(rows);
      });
    });

    // --- NEW BIGINT HANDLING CODE ---
    // Safely stringify the results, converting any BigInt to a string
    const safeJson = JSON.stringify({ data: results }, (key, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );

    res.setHeader('Content-Type', 'application/json');
    return res.status(200).send(safeJson);
    // return res.status(200).json({ data: results });

  } catch (error) {
    console.error('Execution error:', error);
    return res.status(500).json({ error: error.message });
  } finally {
    // 4. Safely close the database connection
    if (db) {
      await new Promise((resolve) => {
        db.close(() => resolve()); // We resolve immediately even if closing throws an error
      });
    }
  }
}