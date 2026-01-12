import type {
  OperationType,
  OperationMetrics,
  PercentileStats,
  OperationStats,
  ErrorBreakdown,
  ErrorSample,
} from './types.js';

const ALL_OPERATIONS: OperationType[] = [
  'push',
  'pop',
  'complete',
  'release',
  'activePush',
  'activeRemove',
  'activeCount',
  'teamQueueCount',
];

const MAX_ERROR_SAMPLES = 100; // Keep last N error samples

export class MetricsCollector {
  private operations: Map<OperationType, OperationMetrics> = new Map();
  private bufferSize: number;
  private errorBreakdown: ErrorBreakdown = {
    http4xx: 0,
    http5xx: 0,
    network: 0,
    timeout: 0,
    other: 0,
  };
  private errorSamples: ErrorSample[] = [];
  private startTime: number = 0;

  constructor(bufferSize: number = 1_000_000) {
    this.bufferSize = bufferSize;

    // Pre-allocate typed arrays for each operation
    for (const op of ALL_OPERATIONS) {
      this.operations.set(op, {
        operationType: op,
        latencies: new Float64Array(bufferSize),
        count: 0,
        successCount: 0,
        errorCount: 0,
        totalLatencyMs: 0,
      });
    }
  }

  start(): void {
    this.startTime = performance.now();
  }

  record(
    op: OperationType,
    latencyMs: number,
    success: boolean,
    httpStatus?: number,
    errorMessage?: string,
    responseBody?: string,
  ): void {
    const metrics = this.operations.get(op);
    if (!metrics) return;

    // Store latency if buffer has space
    if (metrics.count < this.bufferSize) {
      metrics.latencies[metrics.count] = latencyMs;
    }

    metrics.count++;
    metrics.totalLatencyMs += latencyMs;

    if (success) {
      metrics.successCount++;
    } else {
      metrics.errorCount++;

      // Store error sample (keep last N)
      const sample: ErrorSample = {
        timestamp: Date.now(),
        operationType: op,
        httpStatus,
        errorMessage: errorMessage ?? 'Unknown error',
        responseBody: responseBody?.substring(0, 500), // Truncate long bodies
      };
      this.errorSamples.push(sample);
      if (this.errorSamples.length > MAX_ERROR_SAMPLES) {
        this.errorSamples.shift();
      }

      // Categorize error
      if (httpStatus !== undefined) {
        if (httpStatus >= 400 && httpStatus < 500) {
          this.errorBreakdown.http4xx++;
        } else if (httpStatus >= 500) {
          this.errorBreakdown.http5xx++;
        }
      } else if (errorMessage) {
        if (errorMessage.includes('timeout') || errorMessage.includes('ETIMEDOUT')) {
          this.errorBreakdown.timeout++;
        } else if (
          errorMessage.includes('ECONNREFUSED') ||
          errorMessage.includes('ENOTFOUND') ||
          errorMessage.includes('fetch failed')
        ) {
          this.errorBreakdown.network++;
        } else {
          this.errorBreakdown.other++;
        }
      } else {
        this.errorBreakdown.other++;
      }
    }
  }

  calculatePercentiles(op: OperationType): PercentileStats {
    const metrics = this.operations.get(op);
    if (!metrics || metrics.count === 0) {
      return { p50: 0, p95: 0, p99: 0, min: 0, max: 0, mean: 0 };
    }

    const actualCount = Math.min(metrics.count, this.bufferSize);

    // Create a view of actual data and sort
    const data = metrics.latencies.subarray(0, actualCount);
    const sorted = new Float64Array(data).sort();

    const p50Idx = Math.floor(actualCount * 0.50);
    const p95Idx = Math.floor(actualCount * 0.95);
    const p99Idx = Math.floor(actualCount * 0.99);

    return {
      p50: sorted[p50Idx] ?? 0,
      p95: sorted[p95Idx] ?? 0,
      p99: sorted[p99Idx] ?? 0,
      min: sorted[0] ?? 0,
      max: sorted[actualCount - 1] ?? 0,
      mean: metrics.totalLatencyMs / metrics.count,
    };
  }

  getOperationStats(op: OperationType): OperationStats {
    const metrics = this.operations.get(op);
    if (!metrics || metrics.count === 0) {
      return {
        totalRequests: 0,
        successCount: 0,
        errorCount: 0,
        successRate: 0,
        percentiles: { p50: 0, p95: 0, p99: 0, min: 0, max: 0, mean: 0 },
      };
    }

    return {
      totalRequests: metrics.count,
      successCount: metrics.successCount,
      errorCount: metrics.errorCount,
      successRate: metrics.count > 0 ? metrics.successCount / metrics.count : 0,
      percentiles: this.calculatePercentiles(op),
    };
  }

  getAllOperationStats(): Record<OperationType, OperationStats> {
    const stats: Partial<Record<OperationType, OperationStats>> = {};
    for (const op of ALL_OPERATIONS) {
      stats[op] = this.getOperationStats(op);
    }
    return stats as Record<OperationType, OperationStats>;
  }

  getTotalOperations(): number {
    let total = 0;
    for (const metrics of this.operations.values()) {
      total += metrics.count;
    }
    return total;
  }

  getTotalSuccesses(): number {
    let total = 0;
    for (const metrics of this.operations.values()) {
      total += metrics.successCount;
    }
    return total;
  }

  getTotalErrors(): number {
    let total = 0;
    for (const metrics of this.operations.values()) {
      total += metrics.errorCount;
    }
    return total;
  }

  getOverallSuccessRate(): number {
    const total = this.getTotalOperations();
    if (total === 0) return 0;
    return this.getTotalSuccesses() / total;
  }

  getErrorBreakdown(): ErrorBreakdown {
    return { ...this.errorBreakdown };
  }

  getErrorSamples(): ErrorSample[] {
    return [...this.errorSamples];
  }

  getRecentErrors(count: number = 10): ErrorSample[] {
    return this.errorSamples.slice(-count);
  }

  getElapsedMs(): number {
    return performance.now() - this.startTime;
  }

  getOpsPerSecond(): number {
    const elapsedSeconds = this.getElapsedMs() / 1000;
    if (elapsedSeconds === 0) return 0;
    return this.getTotalOperations() / elapsedSeconds;
  }

  // Get raw metrics for a specific operation (for live reporting)
  getRawMetrics(op: OperationType): OperationMetrics | undefined {
    return this.operations.get(op);
  }

  // Reset all metrics (useful for warm-up phases)
  reset(): void {
    for (const metrics of this.operations.values()) {
      metrics.latencies.fill(0);
      metrics.count = 0;
      metrics.successCount = 0;
      metrics.errorCount = 0;
      metrics.totalLatencyMs = 0;
    }
    this.errorBreakdown = {
      http4xx: 0,
      http5xx: 0,
      network: 0,
      timeout: 0,
      other: 0,
    };
    this.startTime = performance.now();
  }
}
