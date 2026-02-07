import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { config } from "../../../config";
import { Meta } from "..";
import { Document } from "../../../controllers/v1/types";

// Lazily initialized S3 client (only created when env vars are present)
let s3Client: S3Client | null = null;

function getS3Client(): S3Client | null {
  if (s3Client) return s3Client;

  if (
    config.SCREENSHOT_S3_ENDPOINT &&
    config.SCREENSHOT_S3_ACCESS_KEY_ID &&
    config.SCREENSHOT_S3_SECRET_ACCESS_KEY &&
    config.SCREENSHOT_S3_BUCKET
  ) {
    s3Client = new S3Client({
      endpoint: config.SCREENSHOT_S3_ENDPOINT,
      region: config.SCREENSHOT_S3_REGION ?? "auto",
      credentials: {
        accessKeyId: config.SCREENSHOT_S3_ACCESS_KEY_ID,
        secretAccessKey: config.SCREENSHOT_S3_SECRET_ACCESS_KEY,
      },
    });
    return s3Client;
  }

  return null;
}

export function uploadScreenshot(meta: Meta, document: Document): Document {
  if (
    document.screenshot === undefined ||
    !document.screenshot.startsWith("data:")
  ) {
    return document;
  }

  const client = getS3Client();

  if (client && config.SCREENSHOT_S3_BUCKET) {
    const fileName = `screenshots/screenshot-${crypto.randomUUID()}.png`;
    const base64Data = document.screenshot.split(",")[1];
    const contentType =
      document.screenshot.split(":")[1]?.split(";")[0] ?? "image/png";

    meta.logger.debug("Uploading screenshot to S3-compatible storage...", {
      bucket: config.SCREENSHOT_S3_BUCKET,
      key: fileName,
    });

    // Fire-and-forget upload (same pattern as original Supabase upload)
    client
      .send(
        new PutObjectCommand({
          Bucket: config.SCREENSHOT_S3_BUCKET,
          Key: fileName,
          Body: Buffer.from(base64Data, "base64"),
          ContentType: contentType,
          CacheControl: "public, max-age=31536000, immutable",
        }),
      )
      .then(() => {
        meta.logger.debug("Screenshot uploaded to S3 successfully.", {
          key: fileName,
        });
      })
      .catch(err => {
        meta.logger.error("Failed to upload screenshot to S3.", {
          error: err?.message ?? String(err),
          key: fileName,
        });
      });

    // Replace base64 data URI with the public URL
    const publicBase = config.SCREENSHOT_S3_PUBLIC_URL?.replace(/\/+$/, "");
    if (publicBase) {
      document.screenshot = `${publicBase}/${fileName}`;
    } else {
      // Fallback: construct URL from endpoint + bucket
      const endpoint = config.SCREENSHOT_S3_ENDPOINT?.replace(/\/+$/, "");
      document.screenshot = `${endpoint}/${config.SCREENSHOT_S3_BUCKET}/${fileName}`;
    }
  }

  // Original Supabase path (cloud mode only) - kept as fallback
  // This only runs if S3 is not configured AND cloud auth is enabled
  if (
    !getS3Client() &&
    config.USE_DB_AUTHENTICATION &&
    document.screenshot.startsWith("data:")
  ) {
    try {
      const { supabase_service } = require("../../../services/supabase");
      meta.logger.debug("Uploading screenshot to Supabase...");

      const fileName = `screenshot-${crypto.randomUUID()}.png`;

      supabase_service.storage
        .from("media")
        .upload(
          fileName,
          Buffer.from(document.screenshot.split(",")[1], "base64"),
          {
            cacheControl: "3600",
            upsert: false,
            contentType: document.screenshot.split(":")[1].split(";")[0],
          },
        );

      document.screenshot = `https://service.firecrawl.dev/storage/v1/object/public/media/${encodeURIComponent(fileName)}`;
    } catch {
      // Supabase not available, leave as base64
    }
  }

  return document;
}
