import { describe, expect, it } from "vitest";

import { normalizeBucket, normalizeQueueMetrics, type RawQueueMetricsBucket } from "./normalize";

const now = "2026-07-04T12:00:00.000Z";

function bucket(overrides: Partial<RawQueueMetricsBucket>): RawQueueMetricsBucket {
  return {
    count: 1,
    createdAtBucket: "2026-07-04T11:00:00.000Z",
    latency: null,
    queue: "process_torrent",
    ranAtBucket: null,
    status: "pending",
    ...overrides,
  };
}

function value(point: Record<string, unknown>, key: string) {
  return point[key];
}

describe("normalizeBucket", () => {
  it("aligns dates to the selected duration and multiplier", () => {
    expect(
      normalizeBucket("2026-07-04T12:34:56.000Z", {
        duration: "hour",
        multiplier: 1,
      }).start.toISOString(),
    ).toBe("2026-07-04T12:00:00.000Z");

    expect(
      normalizeBucket("2026-07-04T07:59:59.000Z", {
        duration: "hour",
        multiplier: 2,
      }).start.toISOString(),
    ).toBe("2026-07-04T06:00:00.000Z");
  });
});

describe("normalizeQueueMetrics", () => {
  it("returns a zero-filled timeframe for empty input", () => {
    const result = normalizeQueueMetrics([], {
      duration: "hour",
      now,
      timeframe: "hours_1",
    });

    expect(result.points).toHaveLength(2);
    expect(result.points.map((point) => point.bucketStart)).toEqual([
      "2026-07-04T11:00:00.000Z",
      "2026-07-04T12:00:00.000Z",
    ]);
    expect(result.eventSeries).toEqual([]);
    expect(result.totals.total).toBe(0);
  });

  it("normalizes a single pending bucket into created and status series", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 5,
          createdAtBucket: "2026-07-04T11:15:00.000Z",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_1",
      },
    );

    expect(result.eventSeries.map((series) => series.dataKey)).toEqual([
      "event:process_torrent:created",
    ]);
    expect(value(result.points[0], "event:process_torrent:created")).toBe(5);
    expect(value(result.points[1], "event:process_torrent:created")).toBe(0);
    expect(value(result.statusPoints[0], "status:process_torrent:pending")).toBe(5);
    expect(result.totals.byQueue).toEqual([
      {
        failed: 0,
        pending: 5,
        processed: 0,
        queue: "process_torrent",
        retry: 0,
        total: 5,
      },
    ]);
  });

  it("zero-fills gaps between non-contiguous buckets", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 1,
          createdAtBucket: "2026-07-04T06:20:00.000Z",
        }),
        bucket({
          count: 2,
          createdAtBucket: "2026-07-04T09:05:00.000Z",
        }),
        bucket({
          count: 3,
          createdAtBucket: "2026-07-04T10:10:00.000Z",
          ranAtBucket: "2026-07-04T11:20:00.000Z",
          status: "processed",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_6",
      },
    );

    expect(result.points.map((point) => point.bucketStart)).toEqual([
      "2026-07-04T06:00:00.000Z",
      "2026-07-04T07:00:00.000Z",
      "2026-07-04T08:00:00.000Z",
      "2026-07-04T09:00:00.000Z",
      "2026-07-04T10:00:00.000Z",
      "2026-07-04T11:00:00.000Z",
      "2026-07-04T12:00:00.000Z",
    ]);
    expect(result.points.map((point) => value(point, "event:process_torrent:created"))).toEqual([
      1, 0, 0, 2, 3, 0, 0,
    ]);
    expect(result.points.map((point) => value(point, "event:process_torrent:processed"))).toEqual([
      0, 0, 0, 0, 0, 3, 0,
    ]);
  });

  it("splits event and status series by queue", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 2,
          queue: "process_torrent",
        }),
        bucket({
          count: 4,
          queue: "process_torrent_batch",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_1",
      },
    );

    expect(result.eventSeries.map((series) => series.dataKey)).toEqual([
      "event:process_torrent:created",
      "event:process_torrent_batch:created",
    ]);
    expect(value(result.points[0], "event:process_torrent:created")).toBe(2);
    expect(value(result.points[0], "event:process_torrent_batch:created")).toBe(4);
    expect(result.totals.byQueue.map((queue) => [queue.queue, queue.total])).toEqual([
      ["process_torrent", 2],
      ["process_torrent_batch", 4],
    ]);
  });

  it("filters queues before building series and totals", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 2,
          queue: "process_torrent",
        }),
        bucket({
          count: 4,
          queue: "process_torrent_batch",
        }),
      ],
      {
        duration: "hour",
        now,
        queues: ["process_torrent_batch"],
        timeframe: "hours_1",
      },
    );

    expect(result.eventSeries.map((series) => series.queue)).toEqual(["process_torrent_batch"]);
    expect(result.totals.total).toBe(4);
  });

  it("filters statuses before building series and totals", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 2,
          status: "pending",
        }),
        bucket({
          count: 7,
          createdAtBucket: "2026-07-04T10:00:00.000Z",
          ranAtBucket: "2026-07-04T11:00:00.000Z",
          status: "failed",
        }),
      ],
      {
        duration: "hour",
        now,
        statuses: ["failed"],
        timeframe: "hours_6",
      },
    );

    expect(result.statusSeries.map((series) => series.status)).toEqual(["failed"]);
    expect(result.totals.byStatus).toEqual({
      failed: 7,
      pending: 0,
      processed: 0,
      retry: 0,
    });
  });

  it("uses ran-at buckets for processed and failed status timelines", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 3,
          createdAtBucket: "2026-07-04T10:00:00.000Z",
          ranAtBucket: "2026-07-04T11:00:00.000Z",
          status: "processed",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_6",
      },
    );

    const processedValues = result.statusPoints.map((point) =>
      value(point, "status:process_torrent:processed"),
    );

    expect(processedValues).toEqual([0, 0, 0, 0, 0, 3, 0]);
    expect(value(result.points[4], "event:process_torrent:created")).toBe(3);
    expect(value(result.points[5], "event:process_torrent:processed")).toBe(3);
  });

  it("computes latency averages from summed duration and count", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 2,
          createdAtBucket: "2026-07-04T10:00:00.000Z",
          latency: "PT10S",
          ranAtBucket: "2026-07-04T11:00:00.000Z",
          status: "processed",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_6",
      },
    );

    expect(result.latencySeries.map((series) => series.dataKey)).toEqual([
      "latency:process_torrent",
    ]);
    expect(value(result.points[5], "latency:process_torrent")).toBe(5);
    expect(value(result.points[4], "latency:process_torrent")).toBeNull();
  });

  it("excludes buckets outside the selected timeframe", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 9,
          createdAtBucket: "2026-07-04T04:00:00.000Z",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "hours_1",
      },
    );

    expect(result.points).toHaveLength(2);
    expect(result.eventSeries).toEqual([]);
    expect(result.statusSeries).toEqual([]);
    expect(result.totals.total).toBe(0);
  });

  it("uses raw data boundaries for the all timeframe", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 3,
          createdAtBucket: "2026-07-04T10:30:00.000Z",
        }),
      ],
      {
        duration: "hour",
        now,
        timeframe: "all",
      },
    );

    expect(result.points.map((point) => point.bucketStart)).toEqual([
      "2026-07-04T10:00:00.000Z",
      "2026-07-04T11:00:00.000Z",
      "2026-07-04T12:00:00.000Z",
    ]);
    expect(value(result.points[0], "event:process_torrent:created")).toBe(3);
  });

  it("supports multi-bucket duration multipliers", () => {
    const result = normalizeQueueMetrics(
      [
        bucket({
          count: 6,
          createdAtBucket: "2026-07-04T07:30:00.000Z",
        }),
      ],
      {
        duration: "hour",
        multiplier: 2,
        now,
        timeframe: "hours_6",
      },
    );

    expect(result.points.map((point) => point.bucketStart)).toEqual([
      "2026-07-04T06:00:00.000Z",
      "2026-07-04T08:00:00.000Z",
      "2026-07-04T10:00:00.000Z",
      "2026-07-04T12:00:00.000Z",
    ]);
    expect(value(result.points[0], "event:process_torrent:created")).toBe(6);
  });
});
