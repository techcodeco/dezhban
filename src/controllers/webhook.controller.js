import { redis } from "../config/redis.js";
import timingSafeEqual from "../validation/timingSafeEqual.js";

const STREAM_NAME = process.env.STREAM_NAME || "rubika:updates";
const MAX_PAYLOAD_SIZE = 512 * 1024;
const BATCH_SIZE = 500;
const BATCH_TIMEOUT_MS = 3;

class UltraFastBatchProcessor {
  constructor(redis, streamName, batchSize = 500, timeoutMs = 3) {
    this.redis = redis;
    this.streamName = streamName;
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;
    this.batch = [];
    this.timer = null;
    this.processing = false;
    this.totalProcessed = 0;
    this.totalDropped = 0;
    this.lastLogTime = Date.now();
    this.lastRpm = 0;
    setInterval(() => this.logStats(), 5000);
  }

  logStats() {
    const now = Date.now();
    const elapsed = (now - this.lastLogTime) / 1000;
    const currentRpm = elapsed > 0 ? (this.totalProcessed / elapsed) * 60 : 0;
    this.lastRpm = currentRpm;
    console.log(
      `📊 [${new Date().toISOString()}] Processed: ${this.totalProcessed.toLocaleString()} | ` +
        `RPM: ${Math.round(currentRpm).toLocaleString()} | ` +
        `Queue: ${this.batch.length} | ` +
        `Dropped: ${this.totalDropped}`,
    );

    this.totalProcessed = 0;
    this.lastLogTime = now;
  }

  add(payload) {
    return new Promise((resolve, reject) => {
      this.batch.push({ payload, resolve, reject, timestamp: Date.now() });
      if (this.batch.length >= this.batchSize) {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = null;
        }
        this.flush().catch((err) => console.error("Flush error:", err));
      } else if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = null;
          this.flush().catch((err) => console.error("Flush error:", err));
        }, this.timeoutMs);
      }
    });
  }

  addFireAndForget(payload) {
    this.batch.push({ payload });
    if (this.batch.length >= this.batchSize) {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      this.flush().catch((err) => console.error("Flush error:", err));
    } else if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = null;
        this.flush().catch((err) => console.error("Flush error:", err));
      }, this.timeoutMs);
    }
  }

  async flush() {
    if (this.processing || this.batch.length === 0) return;
    this.processing = true;
    const currentBatch = [...this.batch];
    this.batch = [];
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    try {
      const pipeline = this.redis.pipeline();
      for (const item of currentBatch) {
        pipeline.xadd(
          this.streamName,
          "MAXLEN",
          "~",
          "10000",
          "*",
          "data",
          typeof item.payload === "string"
            ? item.payload
            : JSON.stringify(item.payload),
          "ts",
          Date.now().toString(),
        );
      }
      const results = await pipeline.exec();
      for (let i = 0; i < results.length; i++) {
        const [err, result] = results[i];
        const item = currentBatch[i];

        if (err) {
          if (item.reject) item.reject(err);
          this.totalDropped++;
        } else {
          if (item.resolve) item.resolve(result);
          this.totalProcessed++;
        }
      }
    } catch (error) {
      console.error("Batch flush error:", error);
      for (const item of currentBatch) {
        if (item.reject) item.reject(error);
      }
      this.totalDropped += currentBatch.length;
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

  getStats() {
    return {
      totalProcessed: this.totalProcessed,
      totalDropped: this.totalDropped,
      queueSize: this.batch.length,
      isProcessing: this.processing,
      currentRpm: this.lastRpm,
    };
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
    const streamId = await Promise.race([
      batchProcessor.add({
        payload: jsonPayload,
        timestamp: startTime,
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Queue timeout")), 100),
      ),
    ]);

    const responseTime = Date.now() - startTime;
    set.status = 200;
    set.headers = {
      "X-Response-Time": `${responseTime}ms`,
      "X-Stream-Id": streamId,
      "X-Queue-Size": batchProcessor.batch.length.toString(),
    };
    return {
      ok: true,
      message: "Queued",
      streamId,
      responseTime: `${responseTime}ms`,
    };
  } catch (error) {
    set.status = 500;
    console.error("XADD failed:", error.message);
    return { ok: false, message: "Redis error", error: error.message };
  }
};

export const stats = async () => {
  const stats = batchProcessor.getStats();
  const redisInfo = await redis.info("stats");

  return {
    ...stats,
    redis: {
      connected: true,
      totalCommands:
        redisInfo.match(/total_commands_processed:(\d+)/)?.[1] || 0,
    },
    uptime: process.uptime(),
  };
};

export const status = async () => {
  try {
    const start = Date.now();
    const pong = await redis.ping();
    const latency = Date.now() - start;
    const stats = batchProcessor.getStats();
    return {
      status: "ok",
      redis: pong === "PONG" ? "connected" : "error",
      latency: `${latency}ms`,
      queueLength: stats.queueSize,
      totalProcessed: stats.totalProcessed,
      totalDropped: stats.totalDropped,
      currentRpm: stats.currentRpm,
    };
  } catch (error) {
    return {
      status: "error",
      redis: "disconnected",
      error: error.message,
    };
  }
};

const shutdown = async () => {
  console.log("\n🛑 Shutting down gracefully...");
  console.log("📊 Final stats:", batchProcessor.getStats());

  console.log("⏳ Flushing remaining batches...");
  await batchProcessor.close();
  console.log("✅ All data flushed to Redis");
  await redis.quit();
  console.log("👋 Goodbye!");
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  shutdown();
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
});
