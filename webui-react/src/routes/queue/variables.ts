import type {
  MetricsBucketDuration,
  QueueJobsOrderByField,
  QueueJobsOrderByInput,
  QueueJobsQueryVariables,
  QueueJobStatus,
  QueueMetricsQueryVariables,
  QueuePurgeJobsInput,
} from "../../graphql/generated/graphql";
import { metricTimeframeSeconds, type MetricTimeframe } from "../../metrics/normalize";

export type QueueFilterSelection = {
  queues: string[];
  statuses: QueueJobStatus[];
};

export type QueueOrderSelection = {
  descending: boolean;
  field: QueueJobsOrderByField;
};

function nonEmptyArray<T>(values: readonly T[]) {
  return values.length ? [...values] : undefined;
}

export function createQueueMetricsVariables(
  bucketDuration: MetricsBucketDuration,
  timeframe: MetricTimeframe,
): QueueMetricsQueryVariables {
  return {
    input: {
      bucketDuration,
      startTime:
        timeframe === "all"
          ? undefined
          : new Date(Date.now() - metricTimeframeSeconds[timeframe] * 1000).toISOString(),
    },
  };
}

export function createQueueJobsVariables({
  filters,
  limit,
  order,
  page,
}: {
  filters: QueueFilterSelection;
  limit: number;
  order: QueueOrderSelection;
  page: number;
}): QueueJobsQueryVariables {
  const queueFilter = nonEmptyArray(filters.queues);
  const statusFilter = nonEmptyArray(filters.statuses);
  const orderBy: QueueJobsOrderByInput[] = [
    {
      descending: order.descending,
      field: order.field,
    },
  ];

  if (order.field !== "created_at") {
    orderBy.push({
      descending: order.descending,
      field: "created_at",
    });
  }

  return {
    input: {
      facets: {
        queue: {
          aggregate: true,
          filter: queueFilter,
        },
        status: {
          aggregate: true,
          filter: statusFilter,
        },
      },
      hasNextPage: true,
      limit,
      orderBy,
      page,
      totalCount: true,
    },
  };
}

export function createQueuePurgeInput(filters: QueueFilterSelection): QueuePurgeJobsInput {
  return {
    queues: nonEmptyArray(filters.queues),
    statuses: nonEmptyArray(filters.statuses),
  };
}
