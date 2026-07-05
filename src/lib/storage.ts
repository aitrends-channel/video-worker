import fs from "fs";
import os from "os";
import path from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { randomUUID } from "crypto";
import { S3Client, PutObjectCommand, DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
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
  // Newer aws-sdk-js versions (3.7xx+) default to streaming chunked
  // uploads with CRC32 trailer checksums (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`),
  // which R2 doesn't fully honor for PutObject. The signature R2
  // calculates differs from what the SDK signed, producing a
  // SignatureDoesNotMatch error. Opt out of flexible-checksum
  // streaming so uploads use the simpler signed-payload mode R2
  // handles correctly.
  requestChecksumCalculation: "WHEN_REQUIRED",
  responseChecksumValidation: "WHEN_REQUIRED",
  // R2 accepts virtual-hosted style today but path-style is the
  // documented-stable contract — pinning to path-style avoids
  // future-SDK-default-change regressions.
  forcePathStyle: true,
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
  // Stream the body instead of fs.readFileSync to avoid loading the
  // full file into the JS heap. For an assembled 1080p video that can
  // easily be 100+ MB — a single uploadFile call held that much in
  // memory the moment it ran. PutObjectCommand requires a known
  // ContentLength when given a stream body, so we stat first.
  const { size } = fs.statSync(filePath);
  const body = fs.createReadStream(filePath);
  try {
    await r2.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: storagePath,
      Body: body,
      ContentType: contentType,
      ContentLength: size,
    }));
  } finally {
    body.destroy();
  }
  return `${PUBLIC_URL}/${storagePath}`;
}

// Optional Cloudflare Worker transfer proxy (cf-transfer-worker,
// deployed separately via wrangler — lives outside this repo). When
// configured, source→R2 transfers happen inside Cloudflare's network and
// Render's egress for each transfer drops from the full file size to a
// ~200-byte JSON request. Unset either var to disable entirely.
const CF_TRANSFER_URL = (process.env.CF_TRANSFER_URL ?? "").replace(/\/$/, "");
const CF_TRANSFER_SECRET = process.env.CF_TRANSFER_SECRET ?? "";

export async function uploadFromUrl(storagePath: string, url: string, contentType: string): Promise<string> {
  assertConfigured();
  if (CF_TRANSFER_URL && CF_TRANSFER_SECRET) {
    try {
      const cfRes = await fetch(CF_TRANSFER_URL, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${CF_TRANSFER_SECRET}`,
        },
        body: JSON.stringify({ url, key: storagePath, contentType }),
      });
      if (cfRes.ok) return `${PUBLIC_URL}/${storagePath}`;
      const errText = await cfRes.text().catch(() => "");
      console.warn(`[storage] CF transfer failed (${cfRes.status}) for ${storagePath}: ${errText} — falling back to direct transfer`);
    } catch (e) {
      console.warn(`[storage] CF transfer error for ${storagePath} — falling back to direct transfer:`, e instanceof Error ? e.message : e);
    }
  }
  // Direct fallback: stream the source URL to a temp file, then
  // stream-upload to R2 via uploadFile. The previous implementation
  // buffered the entire response into an ArrayBuffer and then copied it
  // into a Node Buffer — a 50-500 MB KIE video sat in process RSS twice,
  // and with concurrent KIE beats finishing in parallel that was the
  // dominant memory spike on the Render box. Streaming caps the
  // in-flight footprint at the chunk size pipeline carries through (≈64 KB).
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  if (!res.body) throw new Error(`No response body for ${url}`);
  const tmpPath = path.join(os.tmpdir(), `r2-upload-${randomUUID()}.bin`);
  try {
    await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmpPath));
    return await uploadFile(storagePath, tmpPath, contentType);
  } finally {
    try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
  }
}

// Best-effort delete of a single object by R2 key. Mirrors the engine's
// helper at lib/supabase/storage.ts so callers in the worker can clean
// up the previous video file after a successful regeneration upload.
// Quiet — a missing key is not an error.
export async function deleteObject(key: string): Promise<void> {
  if (!BUCKET) return;
  await r2.send(new DeleteObjectsCommand({
    Bucket: BUCKET,
    Delete: { Objects: [{ Key: key }], Quiet: true },
  }));
}

// List all object keys under a prefix. Used by the assembly worker to
// pre-fetch the set of cached beat-encodes in one paginated bucket scan
// instead of HEAD-ing the public URL per beat (293 round-trips on a
// long project, vs ~1-3 list calls).
export async function listKeysWithPrefix(prefix: string): Promise<Set<string>> {
  if (!BUCKET) return new Set();
  const keys = new Set<string>();
  let continuationToken: string | undefined;
  do {
    const res = await r2.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    }));
    for (const obj of res.Contents ?? []) {
      if (obj.Key) keys.add(obj.Key);
    }
    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);
  return keys;
}

// Convert a R2 public URL back to its bucket key. Returns null when the
// URL is unrelated to our R2 bucket so callers can skip the delete.
export function r2KeyFromUrl(url: string): string | null {
  if (!PUBLIC_URL) return null;
  if (!url.startsWith(PUBLIC_URL + "/")) return null;
  return url.slice(PUBLIC_URL.length + 1);
}
