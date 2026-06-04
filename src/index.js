import dotenv from "dotenv";
dotenv.config();
import { Elysia, t } from "elysia";
import { health } from "./controllers/health.controller.js";
import { webhook } from "./controllers/webhook.controller.js";

const WEBHOOK_SECRET_KEY = process.env.WEBHOOK_SECRET_KEY;
const PORT = parseInt(process.env.PORT || "3000", 10);

let WEBHOOK_SECRET_BUF = null;
if (WEBHOOK_SECRET_KEY) {
  WEBHOOK_SECRET_BUF = Buffer.from(WEBHOOK_SECRET_KEY);
} else {
  console.warn(
    "[Gateway] WEBHOOK_SECRET_KEY is not set. Webhook verification will be bypassed.",
  );
}
// --- Elysia App ---
const app = new Elysia()
  .decorate("webhookSecretBuf", WEBHOOK_SECRET_BUF)
  .get("/health", health)
  .post("/webhook/:secret", webhook, {
    params: t.Object({
      secret: t.String(),
    }),
  })

  .listen(PORT, (server) => {
    console.log(`[Gateway] Service is running on port ${server.port}`);
  });

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
