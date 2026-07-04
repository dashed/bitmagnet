import type {
  MetricsBucketDuration,
  QueueJobStatus,
  QueueMetricsBucket,
} from "../graphql/generated/graphql";

export const metricBucketDurations = [
  "day",
  "hour",
  "minute",
] as const satisfies readonly MetricsBucketDuration[];

export const queueMetricEvents = ["created", "processed", "failed"] as const;

export const queueMetricStatuses = [
  "pending",
  "processed",
  "retry",
  "failed",
] as const satisfies readonly QueueJobStatus[];

export const metricTimeframes = [
  "minutes_15",
  "minutes_30",
  "hours_1",
  "hours_6",
  "hours_12",
  "days_1",
  "weeks_1",
  "all",
] as const;

export const metricAutoRefreshIntervals = [
  "off",
  "seconds_10",
  "seconds_30",
  "minutes_1",
  "minutes_5",
] as const;

export const metricBucketSeconds: Record<MetricsBucketDuration, number> = {
  minute: 60,
  hour: 60 * 60,
  day: 60 * 60 * 24,
};

export const metricTimeframeSeconds: Record<MetricTimeframe, number> = {
  minutes_15: 60 * 15,
  minutes_30: 60 * 30,
  hours_1: 60 * 60,
  hours_6: 60 * 60 * 6,
  hours_12: 60 * 60 * 12,
  days_1: 60 * 60 * 24,
  weeks_1: 60 * 60 * 24 * 7,
  all: Infinity,
};

export const metricAutoRefreshSeconds: Record<MetricAutoRefreshInterval, number | null> = {
  off: null,
  seconds_10: 10,
  seconds_30: 30,
  minutes_1: 60,
  minutes_5: 60 * 5,
};

export type QueueMetricEvent = (typeof queueMetricEvents)[number];
export type MetricTimeframe = (typeof metricTimeframes)[number];
export type MetricAutoRefreshInterval = (typeof metricAutoRefreshIntervals)[number];

export type RawQueueMetricsBucket = Pick<
  QueueMetricsBucket,
  "count" | "createdAtBucket" | "latency" | "queue" | "ranAtBucket" | "status"
>;

export type BucketParams = {
  duration: MetricsBucketDuration;
  multiplier: number;
};

export type NormalizedBucket = {
  index: number;
  key: string;
  start: Date;
};

export type NormalizeQueueMetricsOptions = Partial<BucketParams> & {
  now?: Date | number | string;
  queues?: readonly string[];
  statuses?: readonly QueueJobStatus[];
  timeframe?: MetricTimeframe;
};

export type ChartValue = number | string | null;

export type ChartPoint = {
  bucketIndex: number;
  bucketStart: string;
  label: string;
} & Record<string, ChartValue>;

export type ChartSeries = {
  dataKey: string;
  event?: QueueMetricEvent;
  label: string;
  queue: string;
  status?: QueueJobStatus;
  total: number;
};

export type QueueStatusTotals = Record<QueueJobStatus, number>;

export type QueueTotalsPoint = QueueStatusTotals & {
  queue: string;
  total: number;
};

export type QueueMetricsTotals = {
  byQueue: QueueTotalsPoint[];
  byStatus: QueueStatusTotals;
  total: number;
};

export type NormalizedQueueMetrics = {
  bucketIndexes: number[];
  bucketSpan?: {
    end: NormalizedBucket;
    start: NormalizedBucket;
  };
  eventSeries: ChartSeries[];
  latencySeries: ChartSeries[];
  points: ChartPoint[];
  statusPoints: ChartPoint[];
  statusSeries: ChartSeries[];
  totals: QueueMetricsTotals;
};

type MutableSeries = Omit<ChartSeries, "total"> & {
  total: number;
};

type Accumulator = {
  eventSeries: Map<string, MutableSeries>;
  eventValues: Map<string, Map<number, number>>;
  latencySeries: Map<string, MutableSeries>;
  latencyValues: Map<string, Map<number, { count: number; latencySeconds: number }>>;
  statusSeries: Map<string, MutableSeries>;
  statusValues: Map<string, Map<number, number>>;
  totalsByQueue: Map<string, QueueStatusTotals>;
  totalsByStatus: QueueStatusTotals;
};

const emptyStatusTotals = (): QueueStatusTotals => ({
  failed: 0,
  pending: 0,
  processed: 0,
  retry: 0,
});

export function normalizeBucket(
  rawDate: Date | number | string,
  params: BucketParams,
): NormalizedBucket {
  const date = new Date(rawDate);
  const bucketMs = metricBucketSeconds[params.duration] * params.multiplier * 1000;
  const index = Math.floor(date.getTime() / bucketMs);

  return {
    index,
    key: `${index}`,
    start: new Date(index * bucketMs),
  };
}

export function normalizeQueueMetrics(
  rawBuckets: readonly RawQueueMetricsBucket[],
  options: NormalizeQueueMetricsOptions = {},
): NormalizedQueueMetrics {
  const params = normalizeParams(options);
  const filteredBuckets = filterBuckets(rawBuckets, options);
  const bucketSpan = createBucketSpan(
    filteredBuckets,
    params,
    options.timeframe ?? "all",
    options.now,
  );
  const bucketIndexes = bucketSpan ? range(bucketSpan.start.index, bucketSpan.end.index) : [];
  const accumulator = createAccumulator();

  for (const bucket of filteredBuckets) {
    addBucket(accumulator, bucket, params, bucketSpan);
  }

  const eventSeries = sortSeries(accumulator.eventSeries);
  const statusSeries = sortSeries(accumulator.statusSeries);
  const latencySeries = sortSeries(accumulator.latencySeries);
  const points = createPoints(bucketIndexes, params, eventSeries, accumulator.eventValues);
  addLatencyValues(points, latencySeries, accumulator.latencyValues);

  return {
    bucketIndexes,
    bucketSpan,
    eventSeries,
    latencySeries,
    points,
    statusPoints: createPoints(bucketIndexes, params, statusSeries, accumulator.statusValues),
    statusSeries,
    totals: createTotals(accumulator),
  };
}

function normalizeParams(options: NormalizeQueueMetricsOptions): BucketParams {
  return {
    duration: options.duration ?? "hour",
    multiplier: Math.max(1, Math.floor(options.multiplier ?? 1)),
  };
}

function filterBuckets(
  buckets: readonly RawQueueMetricsBucket[],
  options: NormalizeQueueMetricsOptions,
): RawQueueMetricsBucket[] {
  const queueFilter = options.queues ? new Set(options.queues) : undefined;
  const statusFilter = options.statuses ? new Set(options.statuses) : undefined;

  return buckets.filter((bucket) => {
    if (queueFilter && !queueFilter.has(bucket.queue)) {
      return false;
    }

    if (statusFilter && !statusFilter.has(bucket.status)) {
      return false;
    }

    return true;
  });
}

function createBucketSpan(
  rawBuckets: readonly RawQueueMetricsBucket[],
  params: BucketParams,
  timeframe: MetricTimeframe,
  nowValue: Date | number | string | undefined,
) {
  const now = normalizeBucket(nowValue ?? new Date(), params);

  if (timeframe !== "all") {
    return {
      start: normalizeBucket(
        now.start.getTime() - metricTimeframeSeconds[timeframe] * 1000,
        params,
      ),
      end: now,
    };
  }

  const indexes = rawBuckets.flatMap((bucket) => {
    const created = normalizeBucket(bucket.createdAtBucket, params).index;
    const ran = bucket.ranAtBucket ? [normalizeBucket(bucket.ranAtBucket, params).index] : [];
    return [created, ...ran];
  });

  if (!indexes.length) {
    return undefined;
  }

  return {
    start: bucketFromIndex(Math.min(...indexes), params),
    end: bucketFromIndex(Math.max(now.index, ...indexes), params),
  };
}

function bucketFromIndex(index: number, params: BucketParams): NormalizedBucket {
  const bucketMs = metricBucketSeconds[params.duration] * params.multiplier * 1000;

  return {
    index,
    key: `${index}`,
    start: new Date(index * bucketMs),
  };
}

function createAccumulator(): Accumulator {
  return {
    eventSeries: new Map(),
    eventValues: new Map(),
    latencySeries: new Map(),
    latencyValues: new Map(),
    statusSeries: new Map(),
    statusValues: new Map(),
    totalsByQueue: new Map(),
    totalsByStatus: emptyStatusTotals(),
  };
}

function addBucket(
  accumulator: Accumulator,
  bucket: RawQueueMetricsBucket,
  params: BucketParams,
  bucketSpan: NormalizedQueueMetrics["bucketSpan"],
) {
  const count = bucket.count;

  if (count <= 0) {
    return;
  }

  const createdBucket = normalizeBucket(bucket.createdAtBucket, params);
  const ranBucket = bucket.ranAtBucket ? normalizeBucket(bucket.ranAtBucket, params) : undefined;
  const statusBucket = bucket.status === "pending" ? createdBucket : ranBucket;

  if (isInSpan(statusBucket, bucketSpan)) {
    addStatusPoint(accumulator, bucket.queue, bucket.status, statusBucket.index, count);
    addTotals(accumulator, bucket.queue, bucket.status, count);
  }

  if (isInSpan(createdBucket, bucketSpan)) {
    addEventPoint(accumulator, bucket.queue, "created", createdBucket.index, count);
  }

  if (bucket.status === "processed" && isInSpan(ranBucket, bucketSpan)) {
    addEventPoint(accumulator, bucket.queue, "processed", ranBucket.index, count);
    addLatencyPoint(
      accumulator,
      bucket.queue,
      ranBucket.index,
      count,
      durationToSeconds(bucket.latency),
    );
  }

  if (bucket.status === "failed" && isInSpan(ranBucket, bucketSpan)) {
    addEventPoint(accumulator, bucket.queue, "failed", ranBucket.index, count);
    addLatencyPoint(
      accumulator,
      bucket.queue,
      ranBucket.index,
      count,
      durationToSeconds(bucket.latency),
    );
  }
}

function isInSpan(
  bucket: NormalizedBucket | undefined,
  span: NormalizedQueueMetrics["bucketSpan"],
): bucket is NormalizedBucket {
  if (!bucket || !span) {
    return false;
  }

  return bucket.index >= span.start.index && bucket.index <= span.end.index;
}

function addEventPoint(
  accumulator: Accumulator,
  queue: string,
  event: QueueMetricEvent,
  bucketIndex: number,
  count: number,
) {
  const dataKey = eventDataKey(queue, event);
  ensureSeries(accumulator.eventSeries, dataKey, {
    dataKey,
    event,
    label: `${queue}: ${event}`,
    queue,
  });
  addValue(accumulator.eventValues, dataKey, bucketIndex, count);
}

function addStatusPoint(
  accumulator: Accumulator,
  queue: string,
  status: QueueJobStatus,
  bucketIndex: number,
  count: number,
) {
  const dataKey = statusDataKey(queue, status);
  ensureSeries(accumulator.statusSeries, dataKey, {
    dataKey,
    label: `${queue}: ${status}`,
    queue,
    status,
  });
  addValue(accumulator.statusValues, dataKey, bucketIndex, count);
}

function addLatencyPoint(
  accumulator: Accumulator,
  queue: string,
  bucketIndex: number,
  count: number,
  latencySeconds: number,
) {
  if (!latencySeconds) {
    return;
  }

  const dataKey = latencyDataKey(queue);
  ensureSeries(accumulator.latencySeries, dataKey, {
    dataKey,
    label: `${queue}: latency`,
    queue,
  });

  const seriesValues = ensureValues(accumulator.latencyValues, dataKey);
  const current = seriesValues.get(bucketIndex) ?? { count: 0, latencySeconds: 0 };

  seriesValues.set(bucketIndex, {
    count: current.count + count,
    latencySeconds: current.latencySeconds + latencySeconds,
  });
}

function addTotals(accumulator: Accumulator, queue: string, status: QueueJobStatus, count: number) {
  const byQueue = accumulator.totalsByQueue.get(queue) ?? emptyStatusTotals();
  byQueue[status] += count;
  accumulator.totalsByQueue.set(queue, byQueue);
  accumulator.totalsByStatus[status] += count;
}

function ensureSeries(
  series: Map<string, MutableSeries>,
  key: string,
  value: Omit<MutableSeries, "total">,
) {
  if (!series.has(key)) {
    series.set(key, {
      ...value,
      total: 0,
    });
  }
}

function addValue(
  values: Map<string, Map<number, number>>,
  dataKey: string,
  bucketIndex: number,
  count: number,
) {
  const seriesValues = ensureValues(values, dataKey);
  seriesValues.set(bucketIndex, (seriesValues.get(bucketIndex) ?? 0) + count);
}

function ensureValues<T>(values: Map<string, Map<number, T>>, dataKey: string): Map<number, T> {
  const current = values.get(dataKey);

  if (current) {
    return current;
  }

  const next = new Map<number, T>();
  values.set(dataKey, next);
  return next;
}

function sortSeries(series: Map<string, MutableSeries>): ChartSeries[] {
  return Array.from(series.values()).sort((a, b) => {
    if (a.queue !== b.queue) {
      return a.queue < b.queue ? -1 : 1;
    }

    if (a.event && b.event && a.event !== b.event) {
      return queueMetricEvents.indexOf(a.event) - queueMetricEvents.indexOf(b.event);
    }

    if (a.status && b.status && a.status !== b.status) {
      return queueMetricStatuses.indexOf(a.status) - queueMetricStatuses.indexOf(b.status);
    }

    return a.dataKey < b.dataKey ? -1 : 1;
  });
}

function createPoints(
  bucketIndexes: readonly number[],
  params: BucketParams,
  series: readonly ChartSeries[],
  values: Map<string, Map<number, number>>,
): ChartPoint[] {
  return bucketIndexes.map((bucketIndex) => {
    const bucket = bucketFromIndex(bucketIndex, params);
    const point: ChartPoint = {
      bucketIndex,
      bucketStart: bucket.start.toISOString(),
      label: bucket.start.toISOString(),
    };

    for (const nextSeries of series) {
      const value = values.get(nextSeries.dataKey)?.get(bucketIndex) ?? 0;
      point[nextSeries.dataKey] = value;
      (nextSeries as MutableSeries).total += value;
    }

    return point;
  });
}

function addLatencyValues(
  points: ChartPoint[],
  series: readonly ChartSeries[],
  values: Map<string, Map<number, { count: number; latencySeconds: number }>>,
) {
  for (const point of points) {
    for (const nextSeries of series) {
      const value = values.get(nextSeries.dataKey)?.get(point.bucketIndex);
      point[nextSeries.dataKey] =
        value && value.count > 0 ? value.latencySeconds / value.count : null;
    }
  }
}

function createTotals(accumulator: Accumulator): QueueMetricsTotals {
  const byQueue = Array.from(accumulator.totalsByQueue.entries())
    .map(([queue, totals]) => ({
      ...totals,
      queue,
      total: queueMetricStatuses.reduce((sum, status) => sum + totals[status], 0),
    }))
    .sort((a, b) => a.queue.localeCompare(b.queue));

  return {
    byQueue,
    byStatus: accumulator.totalsByStatus,
    total: queueMetricStatuses.reduce((sum, status) => sum + accumulator.totalsByStatus[status], 0),
  };
}

function eventDataKey(queue: string, event: QueueMetricEvent) {
  return `event:${queue}:${event}`;
}

function statusDataKey(queue: string, status: QueueJobStatus) {
  return `status:${queue}:${status}`;
}

function latencyDataKey(queue: string) {
  return `latency:${queue}`;
}

function range(start: number, end: number) {
  const values: number[] = [];

  for (let index = start; index <= end; index += 1) {
    values.push(index);
  }

  return values;
}

function durationToSeconds(duration: string | null | undefined): number {
  if (!duration) {
    return 0;
  }

  const matches = duration
    .replace(",", ".")
    .match(
      /^P(?:(?<years>\d+(?:\.\d+)?)Y)?(?:(?<months>\d+(?:\.\d+)?)M)?(?:(?<weeks>\d+(?:\.\d+)?)W)?(?:(?<days>\d+(?:\.\d+)?)D)?(?:T(?:(?<hours>\d+(?:\.\d+)?)H)?(?:(?<minutes>\d+(?:\.\d+)?)M)?(?:(?<seconds>\d+(?:\.\d+)?)S)?)?$/,
    );

  if (!matches?.groups) {
    return 0;
  }

  const value = (key: string) => Number.parseFloat(matches.groups?.[key] ?? "0") || 0;

  return (
    value("seconds") +
    value("minutes") * 60 +
    value("hours") * 60 * 60 +
    value("days") * 60 * 60 * 24 +
    value("weeks") * 60 * 60 * 24 * 7 +
    value("months") * 60 * 60 * 24 * 30 +
    value("years") * 60 * 60 * 24 * 365
  );
}
