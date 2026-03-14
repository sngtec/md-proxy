// api/export.js
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { randomUUID } from "crypto";
import { getOrCreateInstance } from "./instanceCache.js";

const ALLOWED_FORMATS = ["csv", "parquet", "json"];
const PRESIGNED_URL_EXPIRY_SECONDS = 5 * 60; // 5 minutes

function getS3Client() {
  const region = process.env.EXPORT_S3_REGION || "eu-central-1";
  return new S3Client({ region });
}

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

  const { sql, format = "csv", db: dbName } = req.body;
  if (!sql) {
    return res
      .status(400)
      .json({ error: 'Missing "sql" property in request body.' });
  }

  const normalizedFormat = format.toLowerCase();
  if (!ALLOWED_FORMATS.includes(normalizedFormat)) {
    return res.status(400).json({
      error: `Invalid format "${format}". Allowed: ${ALLOWED_FORMATS.join(", ")}`,
    });
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res
      .status(401)
      .json({ error: "Missing or invalid Authorization header." });
  }
  const token = authHeader.split(" ")[1];

  const bucket = process.env.EXPORT_S3_BUCKET;
  if (!bucket) {
    return res
      .status(500)
      .json({ error: "Server misconfiguration: S3 bucket not configured." });
  }

  const exportId = randomUUID();
  const s3Key = `exports/${exportId}.${normalizedFormat}`;
  const s3Path = `s3://${bucket}/${s3Key}`;

  try {
    const instance = await getOrCreateInstance(token, dbName);
    const connection = await instance.connect();

    try {
      // Configure S3 credentials for DuckDB's httpfs extension
      const awsKeyId = process.env.AWS_ACCESS_KEY_ID;
      const awsSecret = process.env.AWS_SECRET_ACCESS_KEY;
      const region = process.env.EXPORT_S3_REGION || "eu-central-1";

      if (awsKeyId && awsSecret) {
        await connection.run(`SET s3_access_key_id='${awsKeyId}'`);
        await connection.run(`SET s3_secret_access_key='${awsSecret}'`);
      }
      await connection.run(`SET s3_region='${region}'`);

      // Execute COPY TO S3
      const copySQL = `COPY (${sql}) TO '${s3Path}'`;
      await connection.run(copySQL);
    } finally {
      connection.closeSync();
    }

    // Generate a presigned GET URL
    const s3Client = getS3Client();
    const command = new GetObjectCommand({ Bucket: bucket, Key: s3Key });
    const url = await getSignedUrl(s3Client, command, {
      expiresIn: PRESIGNED_URL_EXPIRY_SECONDS,
    });

    return res.status(200).json({ url });
  } catch (error) {
    console.error("Export error:", error);
    return res.status(500).json({ error: error.message });
  }
}
