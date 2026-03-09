// api/query.js
import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";
import { LRUCache } from "lru-cache";

// 1. Ensure the extension directory exists once
const extDir = "/tmp/duckdb_extensions";
if (!fs.existsSync(extDir)) {
  fs.mkdirSync(extDir, { recursive: true });
}

// 2. Set up the LRU Cache for DuckDB instances
const instanceCache = new LRUCache({
  max: 10, // Max number of concurrent client connections to keep warm
  ttl: 1000 * 60 * 30, // 30 minutes. If a connection sits idle, evict it.

  // This runs automatically whenever an instance is evicted from the cache!
  dispose: (instance, cacheKey, reason) => {
    // We only log the first 12 characters of the token for security!
    const maskedToken = cacheKey.substring(0, 12) + "...";
    console.log(
      `[Cache Cleanup] Closing instance for token ${maskedToken}. Reason: ${reason}`,
    );
    try {
      instance.closeSync();
    } catch (err) {
      console.error(
        `[Cache Cleanup] Error closing instance: ${err.message}`,
      );
    }
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
  const { sql, params, method = "all", db: dbName } = req.body;
  // params can be an array (positional) or an object (named), default to empty array
  const resolvedParams = params ?? [];
  const isNamedParams =
    resolvedParams && !Array.isArray(resolvedParams) && typeof resolvedParams === "object";
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

  const cacheKey = `${token}-${dbName}`;

  try {
    let instance = instanceCache.get(cacheKey);

    if (!instance) {
      instance = await DuckDBInstance.create(
        `md:${dbName || ""}?motherduck_token=${token}`,
        { extension_directory: extDir },
      );
      instanceCache.set(cacheKey, instance);
    }

    const connection = await instance.connect();
    let results;

    try {
      if (method === "batch") {
        // 1. High-speed batching: compile once, run per row

        for (const row of resolvedParams) {
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

          await connection.run(sql, processedArgs);
        }

        results = {
          message: `Successfully executed batch of ${resolvedParams.length} queries.`,
        };
      } else if (method === "exec") {
        // 2. Multi-statement raw scripts (no parameters allowed here)
        await connection.run(sql);
        results = {
          message: "Multi-statement script executed successfully.",
        };
      } else {
        // 3. Default: Single query with or without parameters
        // Supports both positional ($1, $2, ... with array) and named ($name with object)
        const hasParams = isNamedParams || resolvedParams.length > 0;
        const reader = await connection.runAndReadAll(
          sql,
          ...(hasParams ? [resolvedParams] : []),
        );
        results = reader.getRowObjectsJson();
      }
    } finally {
      connection.closeSync();
    }

    // Safely stringify the results to handle BigInts
    const safeJson = JSON.stringify({ data: results }, (key, value) =>
      typeof value === "bigint"
        ? value <= BigInt(Number.MAX_SAFE_INTEGER)
          ? Number(value)
          : value.toString()
        : value,
    );

    res.setHeader("Content-Type", "application/json");
    return res.status(200).send(safeJson);
  } catch (error) {
    console.error("Execution error:", error);
    return res.status(500).json({ error: error.message });
  }
}
