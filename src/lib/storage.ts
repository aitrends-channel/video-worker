import fs from "fs";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { supabase } from "./supabase.js";

// user_id → folder-name cache. The worker uploads on behalf of users
// identified only by their UUID (via the project row), but we want
// human-readable folders in R2 keyed by email. Each lookup hits
// supabase.auth.admin once, then we serve from this Map.
const userFolderCache = new Map<string, string>();

export async function userFolderForId(userId: string): Promise<string> {
  const cached = userFolderCache.get(userId);
  if (cached) return cached;
  try {
    const { data } = await supabase.auth.admin.getUserById(userId);
    const email = data.user?.email;
    const folder = (email ?? userId).trim().toLowerCase();
    userFolderCache.set(userId, folder);
    return folder;
  } catch {
    // Fall back to user_id on lookup failure rather than failing the upload.
    userFolderCache.set(userId, userId);
    return userId;
  }
}

// R2 client — matches the engine's lib/supabase/storage.ts setup so both
// services write to the same bucket with the same credentials, and the
// final assembled videos benefit from R2's zero egress fees + larger
// per-file limits than Supabase Storage.
const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID ?? "",
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY ?? "",
  },
});

const BUCKET = process.env.R2_BUCKET_NAME;
const PUBLIC_URL = (process.env.R2_PUBLIC_URL ?? "").replace(/\/$/, "");

function assertConfigured() {
  if (!BUCKET) throw new Error("R2 storage is not configured — R2_BUCKET_NAME environment variable is missing");
  if (!PUBLIC_URL) throw new Error("R2 storage is not configured — R2_PUBLIC_URL environment variable is missing");
}

export async function uploadBuffer(path: string, buffer: ArrayBuffer, contentType: string): Promise<string> {
  assertConfigured();
  await r2.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: path,
    Body: Buffer.from(buffer),
    ContentType: contentType,
  }));
  return `${PUBLIC_URL}/${path}`;
}

export async function uploadFile(storagePath: string, filePath: string, contentType: string): Promise<string> {
  assertConfigured();
  const body = fs.readFileSync(filePath);
  await r2.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: storagePath,
    Body: body,
    ContentType: contentType,
  }));
  return `${PUBLIC_URL}/${storagePath}`;
}

export async function uploadFromUrl(path: string, url: string, contentType: string): Promise<string> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}`);
  const buffer = await res.arrayBuffer();
  return uploadBuffer(path, buffer, contentType);
}
