import fs from "fs";
import { supabase } from "./supabase.js";

export async function uploadBuffer(path: string, buffer: ArrayBuffer, contentType: string): Promise<string> {
  const { error } = await supabase.storage
    .from("assets")
    .upload(path, new Uint8Array(buffer), { contentType, upsert: true });

  if (error) throw new Error(`Storage upload failed: ${error.message}`);
  const { data: url } = supabase.storage.from("assets").getPublicUrl(path);
  return url.publicUrl;
}

export async function uploadFile(storagePath: string, filePath: string, contentType: string): Promise<string> {
  const stream = fs.createReadStream(filePath);
  const { error } = await supabase.storage
    .from("assets")
    .upload(storagePath, stream, { contentType, upsert: true });

  if (error) throw new Error(`Storage upload failed: ${error.message}`);
  const { data: url } = supabase.storage.from("assets").getPublicUrl(storagePath);
  return url.publicUrl;
}

export async function uploadFromUrl(path: string, url: string, contentType: string): Promise<string> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}`);
  const buffer = await res.arrayBuffer();
  return uploadBuffer(path, buffer, contentType);
}
