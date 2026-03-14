// api/export.test.js
import { describe, it, expect, vi, beforeEach } from "vitest";

// --- Mocks ---

const mockConnection = {
  run: vi.fn().mockResolvedValue(undefined),
  closeSync: vi.fn(),
};

const mockInstance = {
  connect: vi.fn().mockResolvedValue(mockConnection),
};

vi.mock("./instanceCache.js", () => ({
  getOrCreateInstance: vi.fn().mockResolvedValue(mockInstance),
}));

const MockS3Client = vi.fn().mockImplementation(function () {
  return {};
});
const MockGetObjectCommand = vi.fn().mockImplementation(function (params) {
  return params;
});
vi.mock("@aws-sdk/client-s3", () => ({
  S3Client: MockS3Client,
  GetObjectCommand: MockGetObjectCommand,
}));

vi.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: vi.fn().mockResolvedValue("https://s3.example.com/presigned-url"),
}));

vi.mock("crypto", () => ({
  randomUUID: vi.fn().mockReturnValue("test-uuid-1234"),
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
      sql: "SELECT * FROM my_table",
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
  const { default: handler } = await import("./export.js");
  const req = makeReq(reqOverrides);
  const res = makeRes();
  await handler(req, res);
  return res;
}

// --- Tests ---

describe("export handler", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    process.env.PROXY_API_KEY = "test-key";
    process.env.EXPORT_S3_BUCKET = "my-export-bucket";
    process.env.EXPORT_S3_REGION = "eu-central-1";
    process.env.AWS_ACCESS_KEY_ID = "AKIATEST";
    process.env.AWS_SECRET_ACCESS_KEY = "secret123";
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

    it("returns 400 for invalid export format", async () => {
      const res = await callHandler({ body: { format: "xlsx" } });
      expect(res.statusCode).toBe(400);
      expect(res.body.error).toMatch(/Invalid format/);
    });

    it("returns 500 when S3 bucket is not configured", async () => {
      delete process.env.EXPORT_S3_BUCKET;
      const res = await callHandler();
      expect(res.statusCode).toBe(500);
      expect(res.body.error).toMatch(/S3 bucket not configured/);
    });
  });

  describe("successful export", () => {
    it("returns presigned URL for csv export", async () => {
      const res = await callHandler({
        body: { sql: "SELECT * FROM users" },
      });

      expect(res.statusCode).toBe(200);
      expect(res.body.url).toBe("https://s3.example.com/presigned-url");
    });

    it("uses csv format by default", async () => {
      await callHandler({
        body: { sql: "SELECT 1" },
      });

      // Check the COPY TO command used csv extension
      const copyCall = mockConnection.run.mock.calls.find(([sql]) =>
        sql.startsWith("COPY"),
      );
      expect(copyCall[0]).toMatch(/\.csv'$/);
    });

    it("supports parquet format", async () => {
      await callHandler({
        body: { sql: "SELECT 1", format: "parquet" },
      });

      const copyCall = mockConnection.run.mock.calls.find(([sql]) =>
        sql.startsWith("COPY"),
      );
      expect(copyCall[0]).toMatch(/\.parquet'$/);
    });

    it("generates correct COPY TO command with UUID-based S3 key", async () => {
      await callHandler({
        body: { sql: "SELECT * FROM users" },
      });

      const copyCall = mockConnection.run.mock.calls.find(([sql]) =>
        sql.startsWith("COPY"),
      );
      expect(copyCall[0]).toBe(
        "COPY (SELECT * FROM users) TO 's3://my-export-bucket/exports/test-uuid-1234.csv'",
      );
    });

    it("configures S3 credentials on the connection", async () => {
      await callHandler({
        body: { sql: "SELECT 1" },
      });

      const setCalls = mockConnection.run.mock.calls.filter(([sql]) =>
        sql.startsWith("SET"),
      );
      expect(setCalls).toEqual(
        expect.arrayContaining([
          ["SET s3_access_key_id='AKIATEST'"],
          ["SET s3_secret_access_key='secret123'"],
          ["SET s3_region='eu-central-1'"],
        ]),
      );
    });

    it("creates presigned URL with correct bucket and key", async () => {
      const { GetObjectCommand } = await import("@aws-sdk/client-s3");
      const { getSignedUrl } = await import(
        "@aws-sdk/s3-request-presigner"
      );

      await callHandler({ body: { sql: "SELECT 1" } });

      expect(GetObjectCommand).toHaveBeenCalledWith({
        Bucket: "my-export-bucket",
        Key: "exports/test-uuid-1234.csv",
      });
      expect(getSignedUrl).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        { expiresIn: 300 },
      );
    });
  });

  describe("connection lifecycle", () => {
    it("closes connection after successful export", async () => {
      await callHandler();
      expect(mockConnection.closeSync).toHaveBeenCalled();
    });

    it("closes connection even when COPY TO throws", async () => {
      mockConnection.run.mockRejectedValueOnce(new Error("S3 write failed"));
      const res = await callHandler();

      expect(res.statusCode).toBe(500);
      expect(res.body.error).toMatch(/S3 write failed/);
      expect(mockConnection.closeSync).toHaveBeenCalled();
    });
  });

  describe("error handling", () => {
    it("returns 500 with error message on DuckDB failure", async () => {
      const { getOrCreateInstance } = await import("./instanceCache.js");
      getOrCreateInstance.mockRejectedValueOnce(
        new Error("Connection failed"),
      );

      const res = await callHandler();
      expect(res.statusCode).toBe(500);
      expect(res.body.error).toMatch(/Connection failed/);
    });
  });
});
