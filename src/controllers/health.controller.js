import { redis } from "../config/redis";

export const health = () => ({
  ok: true,
  service: "gateway",
  developer: "techcode",
  redisConnected: redis.status === "ready",
});
