import dotenv from "dotenv";
dotenv.config();
import { Elysia, t } from "elysia";
import { health } from "./controllers/health.controller.js";
import { webhook, status, stats } from "./controllers/webhook.controller.js";

const WEBHOOK_SECRET_KEY = process.env.WEBHOOK_SECRET_KEY;

const instanceId = parseInt(process.env.NODE_APP_INSTANCE || "0", 10);
const basePort = parseInt(process.env.PORT || "3000", 10);
const PORT = basePort + instanceId;

let WEBHOOK_SECRET_BUF = null;
if (WEBHOOK_SECRET_KEY) {
  WEBHOOK_SECRET_BUF = Buffer.from(WEBHOOK_SECRET_KEY);
} else {
  console.warn(
    "[Gateway] WEBHOOK_SECRET_KEY is not set. Webhook verification will be bypassed.",
  );
}

const app = new Elysia()
  .decorate("webhookSecretBuf", WEBHOOK_SECRET_BUF)
  .get("/health", health)
  .get("/webhookStatus", status)
  .get("/webhookStats", stats)
  .post("/webhook/:secret", webhook, {
    params: t.Object({
      secret: t.String(),
    }),
  });

try {
  const server = app.listen(PORT, () => {
    console.log(`[Gateway] Instance ${instanceId} is running on port ${PORT}`);
  });
} catch (error) {
  console.error("[Gateway] Failed to start:", error);
  process.exit(1);
}

process.on("unhandledRejection", (reason, promise) => {
  console.error(
    "[Gateway] Unhandled Rejection at:",
    promise,
    "reason:",
    reason,
  );
});

process.on("uncaughtException", (error) => {
  console.error("[Gateway] Uncaught Exception:", error);
});

export default app;
