// api/instanceCache.js
import { DuckDBInstance } from "@duckdb/node-api";
import fs from "fs";
import { LRUCache } from "lru-cache";

// Ensure the extension directory exists once
const extDir = "/tmp/duckdb_extensions";
if (!fs.existsSync(extDir)) {
  fs.mkdirSync(extDir, { recursive: true });
}

// LRU Cache for DuckDB instances
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

/**
 * Get or create a cached DuckDB instance for the given token and database.
 * @param {string} token - MotherDuck token
 * @param {string} [dbName] - optional database name
 * @returns {Promise<DuckDBInstance>}
 */
export async function getOrCreateInstance(token, dbName) {
  const cacheKey = `${token}-${dbName}`;
  let instance = instanceCache.get(cacheKey);

  if (!instance) {
    instance = await DuckDBInstance.create(
      `md:${dbName || ""}?motherduck_token=${token}`,
      { extension_directory: extDir },
    );
    instanceCache.set(cacheKey, instance);
  }

  return instance;
}

export { instanceCache };
