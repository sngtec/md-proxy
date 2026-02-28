// api/query.js
import duckdb from 'duckdb';

export default async function handler(req, res) {
  // 1. Only allow POST requests
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  const { sql } = req.body;
  if (!sql) {
    return res.status(400).json({ error: 'Missing "sql" property in request body.' });
  }

  // 2. Grab the MotherDuck token from Vercel Environment Variables
  const token = process.env.MOTHERDUCK_TOKEN;
  if (!token) {
    return res.status(500).json({ error: 'Server misconfiguration: Missing MotherDuck token.' });
  }

  try {
    // 3. Connect and execute the query
    // We wrap the callback-based db.all in a Promise so Vercel doesn't exit early
    const results = await new Promise((resolve, reject) => {
      const db = new duckdb.Database(`md:?motherduck_token=${token}`);
      
      db.all(sql, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    // 4. Return the data to your Forge app!
    return res.status(200).json({ data: results });
    
  } catch (error) {
    console.error('MotherDuck execution error:', error);
    return res.status(500).json({ error: error.message });
  }
}
