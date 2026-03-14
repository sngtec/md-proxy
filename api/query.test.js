// api/query.test.js
import { describe, it, expect, vi, beforeEach } from "vitest";

// --- Mocks ---

const mockReader = {
  getRowObjectsJson: vi.fn(),
};

const mockConnection = {
  run: vi.fn().mockResolvedValue(undefined),
  runAndReadAll: vi.fn().mockResolvedValue(mockReader),
  closeSync: vi.fn(),
};

const mockInstance = {
  connect: vi.fn().mockResolvedValue(mockConnection),
  closeSync: vi.fn(),
};

vi.mock("./instanceCache.js", () => ({
  getOrCreateInstance: vi.fn().mockResolvedValue(mockInstance),
}));

// --- Helpers ---

function makeReq({ method = "POST", headers = {}, body = {} } = {}) {
  return {
    method,
    headers: {
      "x-proxy-api-key": "test-key",
      authorization: "Bearer test-token",
      ...headers,
    },
    body: {
      sql: "SELECT 1",
      ...body,
    },
  };
}

function makeRes() {
  const res = {
    statusCode: null,
    body: null,
    headers: {},
    status(code) {
      res.statusCode = code;
      return res;
    },
    json(data) {
      res.body = data;
      return res;
    },
    send(data) {
      res.body = data;
      return res;
    },
    setHeader(key, value) {
      res.headers[key] = value;
      return res;
    },
  };
  return res;
}

async function callHandler(reqOverrides = {}) {
  const { default: handler } = await import("./query.js");
  const req = makeReq(reqOverrides);
  const res = makeRes();
  await handler(req, res);
  return res;
}

// --- Tests ---

describe("query handler", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.PROXY_API_KEY = "test-key";
    mockReader.getRowObjectsJson.mockReturnValue([{ id: 1 }]);
  });

  describe("request validation", () => {
    it("rejects non-POST requests", async () => {
      const res = await callHandler({ method: "GET" });
      expect(res.statusCode).toBe(405);
      expect(res.body.error).toMatch(/Method not allowed/);
    });

    it("returns 500 when PROXY_API_KEY is not configured", async () => {
      delete process.env.PROXY_API_KEY;
      const res = await callHandler();
      expect(res.statusCode).toBe(500);
      expect(res.body.error).toMatch(/misconfiguration/);
    });

    it("returns 403 when api key does not match", async () => {
      const res = await callHandler({
        headers: { "x-proxy-api-key": "wrong-key" },
      });
      expect(res.statusCode).toBe(403);
      expect(res.body.error).toMatch(/Forbidden/);
    });

    it("returns 400 when sql is missing", async () => {
      const res = await callHandler({ body: { sql: "" } });
      expect(res.statusCode).toBe(400);
      expect(res.body.error).toMatch(/Missing.*sql/);
    });

    it("returns 401 when authorization header is missing", async () => {
      const res = await callHandler({
        headers: { authorization: undefined },
      });
      expect(res.statusCode).toBe(401);
      expect(res.body.error).toMatch(/Authorization/);
    });

    it("returns 401 when authorization is not Bearer", async () => {
      const res = await callHandler({
        headers: { authorization: "Basic abc123" },
      });
      expect(res.statusCode).toBe(401);
    });
  });

  describe("default method (all)", () => {
    it("runs a simple query and returns row objects", async () => {
      mockReader.getRowObjectsJson.mockReturnValue([
        { id: 1, name: "Alice" },
      ]);
      const res = await callHandler({ body: { sql: "SELECT * FROM users" } });

      expect(res.statusCode).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.data).toEqual([{ id: 1, name: "Alice" }]);
      expect(mockConnection.runAndReadAll).toHaveBeenCalledWith(
        "SELECT * FROM users",
      );
    });

    it("passes positional params as array", async () => {
      mockReader.getRowObjectsJson.mockReturnValue([{ id: 1 }]);
      const res = await callHandler({
        body: {
          sql: "SELECT * FROM users WHERE status = $1 AND age > $2",
          params: ["active", 21],
        },
      });

      expect(res.statusCode).toBe(200);
      expect(mockConnection.runAndReadAll).toHaveBeenCalledWith(
        "SELECT * FROM users WHERE status = $1 AND age > $2",
        ["active", 21],
      );
    });

    it("passes named params as object", async () => {
      mockReader.getRowObjectsJson.mockReturnValue([{ id: 1 }]);
      const res = await callHandler({
        body: {
          sql: "SELECT * FROM users WHERE status = $status",
          params: { status: "active" },
        },
      });

      expect(res.statusCode).toBe(200);
      expect(mockConnection.runAndReadAll).toHaveBeenCalledWith(
        "SELECT * FROM users WHERE status = $status",
        { status: "active" },
      );
    });
  });

  describe("exec method", () => {
    it("runs multi-statement SQL", async () => {
      const sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1);";
      const res = await callHandler({
        body: { sql, method: "exec" },
      });

      expect(res.statusCode).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.data.message).toMatch(/executed successfully/);
      expect(mockConnection.run).toHaveBeenCalledWith(sql);
    });
  });

  describe("batch method", () => {
    it("runs batch inserts row by row", async () => {
      const res = await callHandler({
        body: {
          sql: "INSERT INTO t (id, name) VALUES ($1, $2)",
          method: "batch",
          params: [
            [1, "Alice"],
            [2, "Bob"],
          ],
        },
      });

      expect(res.statusCode).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.data.message).toMatch(/batch of 2/);
      expect(mockConnection.run).toHaveBeenCalledTimes(2);
      expect(mockConnection.run).toHaveBeenCalledWith(
        "INSERT INTO t (id, name) VALUES ($1, $2)",
        [1, "Alice"],
      );
      expect(mockConnection.run).toHaveBeenCalledWith(
        "INSERT INTO t (id, name) VALUES ($1, $2)",
        [2, "Bob"],
      );
    });

    it("converts JS arrays to DuckDB list literals", async () => {
      const res = await callHandler({
        body: {
          sql: "INSERT INTO t (id, tags) VALUES ($1, $2)",
          method: "batch",
          params: [[1, ["tag1", "tag2"]]],
        },
      });

      expect(res.statusCode).toBe(200);
      expect(mockConnection.run).toHaveBeenCalledWith(
        "INSERT INTO t (id, tags) VALUES ($1, $2)",
        [1, "['tag1', 'tag2']"],
      );
    });

    it("handles null and numeric values in list literals", async () => {
      await callHandler({
        body: {
          sql: "INSERT INTO t (vals) VALUES ($1)",
          method: "batch",
          params: [[[null, 42, "it's"]]],
        },
      });

      expect(mockConnection.run).toHaveBeenCalledWith(
        "INSERT INTO t (vals) VALUES ($1)",
        ["[NULL, 42, 'it''s']"],
      );
    });
  });

  describe("BigInt handling", () => {
    it("converts safe BigInts to numbers in response", async () => {
      mockReader.getRowObjectsJson.mockReturnValue([{ count: 42n }]);
      const res = await callHandler({ body: { sql: "SELECT count(*)" } });

      expect(res.statusCode).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.data[0].count).toBe(42);
    });

    it("converts unsafe BigInts to strings in response", async () => {
      const big = BigInt("99999999999999999999");
      mockReader.getRowObjectsJson.mockReturnValue([{ big }]);
      const res = await callHandler({ body: { sql: "SELECT big" } });

      expect(res.statusCode).toBe(200);
      const parsed = JSON.parse(res.body);
      expect(parsed.data[0].big).toBe("99999999999999999999");
    });
  });

  describe("connection lifecycle", () => {
    it("closes connection after successful query", async () => {
      await callHandler();
      expect(mockConnection.closeSync).toHaveBeenCalled();
    });

    it("closes connection even when query throws", async () => {
      mockConnection.runAndReadAll.mockRejectedValueOnce(
        new Error("query failed"),
      );
      const res = await callHandler();

      expect(res.statusCode).toBe(500);
      expect(res.body.error).toMatch(/query failed/);
      expect(mockConnection.closeSync).toHaveBeenCalled();
    });
  });

  describe("instance caching", () => {
    it("calls getOrCreateInstance with token and db", async () => {
      const { getOrCreateInstance } = await import("./instanceCache.js");

      await callHandler({
        headers: { authorization: "Bearer my-token" },
        body: { sql: "SELECT 1", db: "mydb" },
      });

      expect(getOrCreateInstance).toHaveBeenCalledWith("my-token", "mydb");
    });
  });
});
