// test-webhook-final.js
import axios from "axios";
import fs from "fs";

class WebhookLoadTester {
  constructor(config) {
    this.url = config.url;
    this.secret = config.secret;
    this.totalRequests = config.totalRequests || 10000;
    this.concurrency = config.concurrency || 50;
    this.timeout = config.timeout || 5000;
    this.results = [];
  }

  generatePayload(index) {
    return {
      update: {
        id: index,
        message: `Load test message ${index}`,
        timestamp: Date.now(),
        random: Math.random().toString(36).substring(7),
      },
    };
  }

  async sendRequest(index) {
    const start = Date.now();
    try {
      const response = await axios.post(
        `${this.url}/${this.secret}`,
        this.generatePayload(index),
        {
          headers: { "Content-Type": "application/json" },
          timeout: this.timeout,
          validateStatus: (status) => status === 200,
        },
      );

      return {
        success: true,
        duration: Date.now() - start,
        status: response.status,
        index,
      };
    } catch (error) {
      return {
        success: false,
        duration: Date.now() - start,
        error: error.message,
        index,
      };
    }
  }

  async run() {
    console.log(`
╔════════════════════════════════════════════════════════════════╗
║                    Webhook Load Test - 10000 RPM              ║
╠════════════════════════════════════════════════════════════════╣
║  Target: ${this.url}
║  Total Requests: ${this.totalRequests.toLocaleString()}
║  Concurrency: ${this.concurrency}
║  Target RPM: ${Math.round(this.totalRequests)} (if completed in 1 min)
╚════════════════════════════════════════════════════════════════╝
    `);

    const startTime = Date.now();

    // تقسیم به batches
    const batches = Math.ceil(this.totalRequests / this.concurrency);

    for (let i = 0; i < batches; i++) {
      const batchStart = i * this.concurrency;
      const batchSize = Math.min(
        this.concurrency,
        this.totalRequests - batchStart,
      );
      const promises = [];

      for (let j = 0; j < batchSize; j++) {
        promises.push(this.sendRequest(batchStart + j));
      }

      const batchResults = await Promise.all(promises);
      this.results.push(...batchResults);

      // نمایش پیشرفت
      const completed = this.results.length;
      const elapsed = (Date.now() - startTime) / 1000;
      const currentRPM = (completed / elapsed) * 60;
      const progress = (completed / this.totalRequests) * 100;

      process.stdout.write(
        `\r📊 ${progress.toFixed(1)}% | ${completed.toLocaleString()}/${this.totalRequests.toLocaleString()} | RPM: ${Math.round(currentRPM)}`,
      );
    }

    const totalTime = (Date.now() - startTime) / 1000;
    this.printResults(totalTime);
    this.saveReport(totalTime);
  }

  printResults(totalTime) {
    const successful = this.results.filter((r) => r.success);
    const failed = this.results.filter((r) => !r.success);
    const durations = successful.map((r) => r.duration).sort((a, b) => a - b);

    const avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length;
    const p50 = durations[Math.floor(durations.length * 0.5)];
    const p95 = durations[Math.floor(durations.length * 0.95)];
    const p99 = durations[Math.floor(durations.length * 0.99)];

    const actualRPM = (successful.length / totalTime) * 60;
    const meetsTarget = actualRPM >= 10000;

    console.log(`\n
╔════════════════════════════════════════════════════════════════╗
║                      Test Results                              ║
╠════════════════════════════════════════════════════════════════╣
║  Total Requests:  ${this.totalRequests.toLocaleString()}
║  Successful:      ${successful.length.toLocaleString()}
║  Failed:          ${failed.length.toLocaleString()}
║  Success Rate:    ${((successful.length / this.totalRequests) * 100).toFixed(2)}%
║                                                                ║
║  Total Time:      ${totalTime.toFixed(2)}s
║  Actual RPM:      ${Math.round(actualRPM)}
║  Actual RPS:      ${(successful.length / totalTime).toFixed(1)}
║  Target Met:      ${meetsTarget ? "✅ YES" : "❌ NO"}
║                                                                ║
║  Response Times:                                               ║
║    Average:       ${avgDuration.toFixed(2)}ms
║    P50:           ${p50.toFixed(2)}ms
║    P95:           ${p95.toFixed(2)}ms
║    P99:           ${p99.toFixed(2)}ms
║    Min:           ${Math.min(...durations)}ms
║    Max:           ${Math.max(...durations)}ms
╚════════════════════════════════════════════════════════════════╝
    `);

    if (meetsTarget) {
      console.log("🎉 CONGRATULATIONS! Your webhook can handle 10,000 RPM!\n");
    } else {
      console.log(
        `⚠️  Your webhook handled ${Math.round(actualRPM)} RPM. Need to handle 10,000 RPM.\n`,
      );
    }
  }

  saveReport(totalTime) {
    const report = {
      timestamp: new Date().toISOString(),
      config: {
        url: this.url,
        totalRequests: this.totalRequests,
        concurrency: this.concurrency,
        timeout: this.timeout,
      },
      results: {
        total: this.results.length,
        successful: this.results.filter((r) => r.success).length,
        failed: this.results.filter((r) => !r.success).length,
        totalTime: totalTime,
        avgResponseTime:
          this.results
            .filter((r) => r.success)
            .reduce((a, b) => a + b.duration, 0) /
          this.results.filter((r) => r.success).length,
        actualRPM:
          (this.results.filter((r) => r.success).length / totalTime) * 60,
      },
    };

    // fs.writeFileSync(
    //   `test-report-${Date.now()}.json`,
    //   JSON.stringify(report, null, 2),
    // );
    console.log(`📄 Detailed report saved to: test-report-${Date.now()}.json`);
  }
}

// ===== اجرای تست =====
const tester = new WebhookLoadTester({
  url: "http://localhost:3000/webhook",
  secret: "test", // 👈 سیکرت خود را وارد کنید
  totalRequests: 100000,
  concurrency: 100, // برای 10000 RPM، 100 کافی است
  timeout: 5000,
});

tester.run().catch(console.error);
