# MotherDuck Serverless Proxy (`md-proxy`)

A lightweight, high-performance serverless HTTP proxy for [MotherDuck](https://motherduck.com/), built to run on Vercel. 

This proxy acts as a bridge for serverless environments (like Atlassian Forge or Cloudflare Workers) that restrict native C++ bindings or cannot run the standard DuckDB Node.js client natively.

## ✨ Features
* **Multi-Tenant:** Accepts MotherDuck tokens dynamically per request.
* **Smart Caching:** Uses an LRU (Least Recently Used) cache to keep database connections "warm" across requests while strictly preventing memory leaks.
* **Three Execution Modes:** Supports standard parameterized queries, multi-statement raw scripts, and high-speed batch inserts.
* **BigInt Safe:** Automatically intercepts and serializes 64-bit integers (`BigInt`) to strings to prevent JSON serialization crashes.
* **Secure:** Locked down via a shared secret API key.

---

## 📖 API Documentation

**Endpoint:** `POST https://<your-vercel-domain>.vercel.app/api/query`

### Required Headers
To make a request, you must provide both the client's MotherDuck service token and the shared proxy API key.

* `Authorization: Bearer <MOTHERDUCK_TOKEN>`
* `x-proxy-api-key: <YOUR_PROXY_API_KEY>`
* `Content-Type: application/json`

### Execution Modes & Payloads

The proxy supports three different methods of executing SQL.

#### 1. Standard Query (Default)
Use this for `SELECT` statements or single updates. You can safely pass parameters using the `?` placeholder to prevent SQL injection.

**Request:**
```json
{
  "sql": "SELECT * FROM users WHERE status = ? AND age > ?",
  "params": ["active", 21]
}
```

#### 2. Batch Execution (method: "batch")

Use this for high-speed, bulk inserts or updates. It utilizes DuckDB prepared statements to compile the query once and execute it over an array of arrays.

**Request:**
```json
{
  "method": "batch",
  "sql": "INSERT INTO my_table (id, name) VALUES (?, ?)",
  "params": [
    [1, "Alice"],
    [2, "Bob"],
    [3, "Charlie"]
  ]
}
```

#### 3. Multi-Statement Scripts (method: "exec")

Use this to run multiple SQL commands separated by semicolons (e.g., setting up temporary tables). Note: Parameters are not supported in exec mode.

**Request:**
```json
{
  "method": "exec",
  "sql": "CREATE TABLE tmp (id INT); INSERT INTO tmp VALUES (1); UPDATE my_table SET count = count + 1;"
}
```

**Response Format**

Successful queries return a 200 OK with a data array containing the rows.
```json
{
  "data": [
    { "id": "1", "name": "Alice" },
    { "id": "2", "name": "Bob" }
  ]
}
```

## 🛠️ Developer Guide (For Maintainers)

This project is designed to be deployed to Vercel as a pure Serverless API Function. It does not require a build step.

### Prerequisites

Node.js installed

Vercel CLI installed (npm i -g vercel)

### Local Setup

Clone the repository and install dependencies:

    npm install

(Optional) To run locally using Vercel dev:

    vercel dev

**Deployment**

Because this relies on the native duckdb binary and Vercel's specific /tmp file system for extensions, it must be deployed to Vercel to function correctly.

Deploy directly to production:

    vercel --prod

Go to your Vercel Dashboard -> Project -> Settings -> Environment Variables.

Add a new variable named PROXY_API_KEY and set it to a secure, random string.

Redeploy the project to apply the environment variables.

## Important Architecture Notes

- **Vercel Read-Only File System:** Standard DuckDB attempts to download MotherDuck extensions to ~/.duckdb. Since Vercel is read-only, the code explicitly overrides this to use Vercel's writable /tmp/duckdb_extensions directory.

- **Memory Management:** Vercel functions are capped at 1024MB RAM on the free tier. The lru-cache is currently configured to hold a maximum of 10 warm connections (max: 10) with a 30-minute TTL. If you expect higher concurrency across different MotherDuck tokens, consider upgrading Vercel memory and increasing the cache size.