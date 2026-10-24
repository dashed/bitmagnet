import type { QueueJobStatus, QueueJobsOrderByField } from "../../graphql/generated/graphql";
import { queueMetricStatuses } from "../../metrics/normalize";
import type { QueueOrderSelection } from "./variables";

export const QUEUE_NAMES = ["process_torrent", "process_torrent_batch"] as const;

export const QUEUE_STATUSES = queueMetricStatuses;

export const QUEUE_PAGE_SIZES = [10, 20, 50, 100] as const;

export const QUEUE_ORDER_OPTIONS = [
  {
    defaultDescending: true,
    field: "ran_at",
  },
  {
    defaultDescending: true,
    field: "created_at",
  },
  {
    defaultDescending: false,
    field: "priority",
  },
] as const satisfies ReadonlyArray<{
  defaultDescending: boolean;
  field: QueueJobsOrderByField;
}>;

export const DEFAULT_QUEUE_ORDER = {
  descending: true,
  field: "ran_at",
} as const satisfies QueueOrderSelection;

export const DEFAULT_QUEUE_PAGE_SIZE = 20;

export function sortQueues(values: readonly string[]) {
  const queueOrder = new Map<string, number>(QUEUE_NAMES.map((queue, index) => [queue, index]));

  return [...values].sort((left, right) => {
    const leftIndex = queueOrder.get(left) ?? Number.MAX_SAFE_INTEGER;
    const rightIndex = queueOrder.get(right) ?? Number.MAX_SAFE_INTEGER;

    return leftIndex - rightIndex || left.localeCompare(right);
  });
}

export function sortStatuses(values: readonly QueueJobStatus[]) {
  const statusOrder = new Map(QUEUE_STATUSES.map((status, index) => [status, index]));

  return [...values].sort((left, right) => statusOrder.get(left)! - statusOrder.get(right)!);
}
