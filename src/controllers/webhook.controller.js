import { redis } from "../config/redis.js";
import timingSafeEqual from "../validation/timingSafeEqual.js";

const STREAM_NAME = process.env.STREAM_NAME || "rubika:updates";
const MAX_PAYLOAD_SIZE = 512 * 1024;
const BATCH_SIZE = 1000;
const BATCH_TIMEOUT_MS = 2;

class UltraFastBatchProcessor {
  constructor(redis, streamName, batchSize = 1000, timeoutMs = 2) {
    this.redis = redis;
    this.streamName = streamName;
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;
    this.batch = [];
    this.timer = null;
    this.processing = false;
  }

  add(payload) {
    return new Promise((resolve, reject) => {
      this.batch.push({ payload, resolve, reject });

      if (this.batch.length >= this.batchSize) {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = null;
        }
        setImmediate(() => this.flush());
      } else if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = null;
          setImmediate(() => this.flush());
        }, this.timeoutMs);
      }
    });
  }

  async flush() {
    if (this.processing || this.batch.length === 0) return;

    this.processing = true;
    const currentBatch = this.batch;
    this.batch = [];

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    try {
      const pipeline = this.redis.pipeline();
      const now = Date.now();

      for (let i = 0; i < currentBatch.length; i++) {
        const item = currentBatch[i];
        const payloadStr =
          typeof item.payload === "string"
            ? item.payload
            : JSON.stringify(item.payload);

        pipeline.xadd(this.streamName, "*", "data", payloadStr, "ts", now);
      }

      const results = await pipeline.exec();

      for (let i = 0; i < results.length; i++) {
        const [err, result] = results[i];
        const item = currentBatch[i];

        if (err) {
          if (item.reject) item.reject(err);
        } else {
          if (item.resolve) item.resolve(result);
        }
      }
    } catch (error) {
      for (const item of currentBatch) {
        if (item.reject) item.reject(error);
      }
    } finally {
      this.processing = false;
      if (this.batch.length > 0) {
        setImmediate(() => this.flush());
      }
    }
  }

  async close() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.flush();
  }
}

const batchProcessor = new UltraFastBatchProcessor(
  redis,
  STREAM_NAME,
  BATCH_SIZE,
  BATCH_TIMEOUT_MS,
);

// فقط Webhook اصلی
export const webhook = async ({ params, request, set, webhookSecretBuf }) => {
  const startTime = Date.now();
  const { secret } = params;

  if (
    !webhookSecretBuf ||
    !timingSafeEqual(Buffer.from(secret), webhookSecretBuf)
  ) {
    set.status = 401;
    return { ok: false, message: "Invalid secret" };
  }

  const contentLength = parseInt(request.headers.get("content-length") || "0");
  if (contentLength > MAX_PAYLOAD_SIZE) {
    set.status = 413;
    return { ok: false, message: "Payload too large" };
  }

  let jsonPayload;
  try {
    const rawBody = await request.text();
    jsonPayload = JSON.parse(rawBody);
  } catch (error) {
    set.status = 400;
    return { ok: false, message: "Invalid JSON" };
  }

  if (!jsonPayload?.update) {
    set.status = 400;
    return { ok: false, message: "Missing update" };
  }

  try {
    const streamId = await Promise.race([
      batchProcessor.add({
        payload: jsonPayload,
        timestamp: startTime,
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Queue timeout")), 50),
      ),
    ]);

    const responseTime = Date.now() - startTime;
    set.status = 200;
    return {
      ok: true,
      streamId,
      responseTime: `${responseTime}ms`,
    };
  } catch (error) {
    set.status = 500;
    return { ok: false, message: "Redis error" };
  }
};

// Shutdown ساده
const shutdown = async () => {
  console.log("🛑 Shutting down...");
  await batchProcessor.close();
  await redis.quit();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
