import { redis } from "../config/redis";
import timingSafeEqual from "../validation/timingSafeEqual";

const STREAM_NAME = process.env.STREAM_NAME || "rubika:updates";
const MAX_PAYLOAD_SIZE = 512 * 1024; // 512KB
const BATCH_SIZE = 200;
const BATCH_TIMEOUT_MS = 5;

class UltraFastBatchProcessor {
  constructor(redis, streamName, batchSize = 200, timeoutMs = 5) {
    this.redis = redis;
    this.streamName = streamName;
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;
    this.batch = [];
    this.timer = null;
    this.processing = false;
    this.totalProcessed = 0;
    this.lastLogTime = Date.now();

    setInterval(() => this.logStats(), 10000);
  }

  logStats() {
    const now = Date.now();
    const elapsed = (now - this.lastLogTime) / 1000;
    const rpm = elapsed > 0 ? (this.totalProcessed / elapsed) * 60 : 0;
    console.log(
      `📊 Stats: ${this.totalProcessed} processed, ${Math.round(rpm)} RPM`,
    );
    this.totalProcessed = 0;
    this.lastLogTime = now;
  }

  async add(data) {
    this.batch.push(data);

    if (this.batch.length >= this.batchSize) {
      if (this.timer) clearTimeout(this.timer);
      return this.flush();
    }

    if (!this.timer) {
      this.timer = setTimeout(() => this.flush(), this.timeoutMs);
    }

    return new Promise((resolve, reject) => {
      data.resolve = resolve;
      data.reject = reject;
    });
  }

  async flush() {
    if (this.processing || this.batch.length === 0) return;

    this.processing = true;
    const currentBatch = [...this.batch];
    this.batch = [];
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;

    try {
      const pipeline = this.redis.pipeline();

      for (const item of currentBatch) {
        pipeline.xadd(
          this.streamName,
          "*",
          "data",
          JSON.stringify(item.payload),
        );
      }

      const results = await pipeline.exec();

      results.forEach(([err, result], index) => {
        if (err) {
          currentBatch[index].reject(err);
        } else {
          currentBatch[index].resolve(result);
          this.totalProcessed++;
        }
      });
    } catch (error) {
      currentBatch.forEach((item) => item.reject(error));
    } finally {
      this.processing = false;
      if (this.batch.length > 0) this.flush();
    }
  }
}

const batchProcessor = new UltraFastBatchProcessor(
  redis,
  STREAM_NAME,
  BATCH_SIZE,
  BATCH_TIMEOUT_MS,
);

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
    const streamId = await batchProcessor.add({
      payload: jsonPayload,
      timestamp: startTime,
    });

    set.status = 200;
    set.headers = {
      "X-Response-Time": `${Date.now() - startTime}µs`,
      "X-Stream-Id": streamId,
    };

    console.log({
      ok: true,
      message: "Queued",
      streamId,
    });
    return {
      ok: true,
      message: "Queued",
      streamId,
    };
  } catch (error) {
    set.status = 500;
    console.error("XADD failed:", error.message);
    return { ok: false, message: "Redis error" };
  }
};

export const health = async () => {
  try {
    const pong = await redis.ping();
    return {
      status: "ok",
      redis: pong === "PONG" ? "connected" : "error",
      queueLength: batchProcessor.batch.length,
    };
  } catch (error) {
    return { status: "error", redis: "disconnected" };
  }
};

const shutdown = async () => {
  console.log("🛑 Shutting down gracefully...");
  await batchProcessor.flush();
  await redis.quit();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
