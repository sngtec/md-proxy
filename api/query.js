// api/query.js
import duckdb from 'duckdb';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  const { sql } = req.body;
  if (!sql) {
    return res.status(400).json({ error: 'Missing "sql" property in request body.' });
  }

  // 1. Extract the token from the Authorization header (Bearer <TOKEN>)
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header.' });
  }
  const token = authHeader.split(' ')[1];

  let db;
  try {
    // 2. Initialize a connection dynamically using the client's token
    db = new duckdb.Database(`md:?motherduck_token=${token}`);

    // 3. Execute the query
    const results = await new Promise((resolve, reject) => {
      db.all(sql, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    return res.status(200).json({ data: results });

  } catch (error) {
    console.error('MotherDuck execution error:', error);
    return res.status(500).json({ error: error.message });
  } finally {
    // 4. CRITICAL: Close the database to free up memory for the next client
    if (db) {
      await new Promise((resolve) => db.close(resolve));
    }
  }
}