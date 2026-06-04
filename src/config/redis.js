import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URI, {
  lazyConnect: false,
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => {
    if (times > 10) {
      console.error("[Redis] Max retries reached. Exiting.");
      process.exit(1);
    }
    return Math.min(times * 100, 3000);
  },
  enableReadyCheck: true,
  keepAlive: 30000,
  family: 0,
});

redis.on("connect", () =>
  console.log("✅ Redis connected via ioredis (RESP2)"),
);
redis.on("error", (err) => console.error("Redis error:", err));

export { redis };
