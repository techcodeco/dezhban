import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URI);

redis.on("error", (err) => console.error("[Gateway] Redis Error:", err));
redis.on("connect", () => console.log("[Gateway] Connected to Redis"));

export { redis };

export const STREAM = "rubikaUpdates:events";
export const GROUP = "rubikaUpdate";
