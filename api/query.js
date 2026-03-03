// api/query.js
import duckdb from "duckdb";
import fs from "fs";
import { LRUCache } from "lru-cache";

// 1. Ensure the extension directory exists once
const extDir = "/tmp/duckdb_extensions";
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
    const maskedToken = token.substring(0, 8) + "...";
    console.log(
      `[Cache Cleanup] Closing connection for token ${maskedToken}. Reason: ${reason}`,
    );
    db.close();
  },
});

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed. Use POST." });
  }

  // Security Check
  const providedKey = req.headers["x-proxy-api-key"];
  const expectedKey = process.env.PROXY_API_KEY;
  if (!expectedKey)
    return res.status(500).json({ error: "Server misconfiguration." });
  if (providedKey !== expectedKey)
    return res.status(403).json({ error: "Forbidden." });

  // We now accept 'params' and an optional 'method' ('all', 'exec', or 'batch')
  const { sql, params = [], method = "all", db: dbName } = req.body;
  if (!sql) {
    return res
      .status(400)
      .json({ error: 'Missing "sql" property in request body.' });
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res
      .status(401)
      .json({ error: "Missing or invalid Authorization header." });
  }
  const token = authHeader.split(" ")[1];

  const connectionCacheKey = `${token}-${dbName}`;

  try {
    let db = connectionCache.get(connectionCacheKey);

    if (!db) {
      db = await new Promise((resolve, reject) => {
        const config = { extension_directory: extDir };
        const database = new duckdb.Database(
          `md:${dbName || ""}?motherduck_token=${token}`,
          config,
          (err) => {
            if (err)
              reject(new Error(`MotherDuck Auth Failed: ${err.message}`));
            else resolve(database);
          },
        );
      });
      connectionCache.set(connectionCacheKey, db);
    }

    let results;

    if (method === "batch") {
      // 1. High-speed batching using a Prepared Statement
      results = await new Promise((resolve, reject) => {
        const stmt = db.prepare(sql);

        for (const row of params) {
          // Ensure each row is spread as individual arguments
          const rowArgs = Array.isArray(row) ? row : [row];

          // Convert JS arrays to DuckDB list literals
          const processedArgs = rowArgs.map((val) => {
            if (!Array.isArray(val)) return val;
            const items = val.map((v) => {
              if (v === null || v === undefined) return "NULL";
              if (typeof v === "string") return `'${v.replace(/'/g, "''")}'`;
              return String(v);
            });
            return `[${items.join(", ")}]`;
          });
          stmt.run(...processedArgs);
        }

        stmt.finalize((err) => {
          if (err) reject(new Error(`Batch Error: ${err.message}`));
          else
            resolve({
              message: `Successfully executed batch of ${params.length} queries.`,
            });
        });
      });
    } else if (method === "exec") {
      // 2. Multi-statement raw scripts (no parameters allowed here)
      results = await new Promise((resolve, reject) => {
        db.exec(sql, (err) => {
          if (err) reject(new Error(`Exec Error: ${err.message}`));
          else
            resolve({
              message: "Multi-statement script executed successfully.",
            });
        });
      });
    } else {
      // 3. Default: Single query with or without parameters
      results = await new Promise((resolve, reject) => {
        // We spread the params array into the function arguments
        db.all(sql, ...params, (err, rows) => {
          if (err) reject(new Error(`Query Error: ${err.message}`));
          else resolve(rows);
        });
      });
    }

    // Safely stringify the results to handle BigInts
    const safeJson = JSON.stringify({ data: results }, (key, value) =>
      typeof value === "bigint" ? value.toString() : value,
    );

    res.setHeader("Content-Type", "application/json");
    return res.status(200).send(safeJson);
  } catch (error) {
    console.error("Execution error:", error);
    return res.status(500).json({ error: error.message });
  }
}
