import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Fragment, useEffect, useMemo, useState, type ChangeEvent } from "react";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../../components/ListSkeleton";
import { QueryError } from "../../components/QueryError";
import { execute } from "../../graphql/client";
import {
  QueueJobsDocument,
  type QueueJobsOrderByField,
  type QueueJobsQuery,
  type QueueJobStatus,
} from "../../graphql/generated/graphql";
import { formatQueueDateTime, formatQueueRelativeTime, prettifyQueuePayload } from "./format";
import {
  DEFAULT_QUEUE_ORDER,
  DEFAULT_QUEUE_PAGE_SIZE,
  QUEUE_NAMES,
  QUEUE_ORDER_OPTIONS,
  QUEUE_PAGE_SIZES,
  QUEUE_STATUSES,
  sortQueues,
  sortStatuses,
} from "./constants";
import {
  createQueueJobsVariables,
  type QueueFilterSelection,
  type QueueOrderSelection,
} from "./variables";
import styles from "../QueuePage.module.css";

type QueueJobsResult = QueueJobsQuery["queue"]["jobs"];
type QueueJob = QueueJobsResult["items"][number];
type QueueAgg = NonNullable<QueueJobsResult["aggregations"]["queue"]>[number];
type StatusAgg = NonNullable<QueueJobsResult["aggregations"]["status"]>[number];
type JobDetailLabels = {
  createdAt: string;
  error: string;
  expand: string;
  id: string;
  notRun: string;
  payload: string;
  priority: string;
  queue: string;
  ranAt: string;
  retries: string;
  runAfter: string;
  status: string;
};
type FacetOption<TValue extends string> = {
  count: number;
  label: string;
  value: TValue;
};

const EMPTY_JOBS: QueueJob[] = [];
const EMPTY_QUEUE_AGGS: QueueAgg[] = [];
const EMPTY_STATUS_AGGS: StatusAgg[] = [];

function toggleQueueFilter(current: readonly string[], value: string) {
  return current.includes(value)
    ? current.filter((item) => item !== value)
    : sortQueues([...current, value]);
}

function toggleStatusFilter(current: readonly QueueJobStatus[], value: QueueJobStatus) {
  return current.includes(value)
    ? current.filter((item) => item !== value)
    : sortStatuses([...current, value]);
}

function isQueueOrderField(value: string): value is QueueJobsOrderByField {
  return QUEUE_ORDER_OPTIONS.some((option) => option.field === value);
}

function mergeQueueOptions(aggregations: readonly QueueAgg[], selectedQueues: readonly string[]) {
  const options = new Map<string, FacetOption<string>>();

  for (const queue of QUEUE_NAMES) {
    options.set(queue, {
      count: 0,
      label: queue,
      value: queue,
    });
  }

  for (const agg of aggregations) {
    options.set(agg.value, {
      count: agg.count,
      label: agg.label || agg.value,
      value: agg.value,
    });
  }

  for (const queue of selectedQueues) {
    if (!options.has(queue)) {
      options.set(queue, {
        count: 0,
        label: queue,
        value: queue,
      });
    }
  }

  return sortQueues(Array.from(options.keys())).map((queue) => options.get(queue)!);
}

function mergeStatusOptions(
  aggregations: readonly StatusAgg[],
  selectedStatuses: readonly QueueJobStatus[],
  getLabel: (status: QueueJobStatus) => string,
) {
  const options = new Map<QueueJobStatus, FacetOption<QueueJobStatus>>();

  for (const status of QUEUE_STATUSES) {
    options.set(status, {
      count: 0,
      label: getLabel(status),
      value: status,
    });
  }

  for (const agg of aggregations) {
    options.set(agg.value, {
      count: agg.count,
      label: getLabel(agg.value),
      value: agg.value,
    });
  }

  for (const status of selectedStatuses) {
    if (!options.has(status)) {
      options.set(status, {
        count: 0,
        label: getLabel(status),
        value: status,
      });
    }
  }

  return sortStatuses(Array.from(options.keys())).map((status) => options.get(status)!);
}

function FacetChipGroup<TValue extends string>({
  allLabel,
  legend,
  onClear,
  onToggle,
  options,
  selected,
}: {
  allLabel: string;
  legend: string;
  onClear: () => void;
  onToggle: (value: TValue) => void;
  options: readonly FacetOption<TValue>[];
  selected: readonly TValue[];
}) {
  const selectedSet = new Set(selected);

  return (
    <div className={styles["facetGroup"]}>
      <span>{legend}</span>
      <div className={styles["chipRow"]}>
        <button
          className={styles["chip"]}
          data-active={selected.length === 0}
          onClick={onClear}
          type="button"
        >
          <span>{allLabel}</span>
        </button>
        {options.map((option) => (
          <button
            className={styles["chip"]}
            data-active={selectedSet.has(option.value)}
            key={option.value}
            onClick={() => onToggle(option.value)}
            type="button"
          >
            <span>{option.label}</span>
            <small>{option.count.toLocaleString()}</small>
          </button>
        ))}
      </div>
    </div>
  );
}

function StatusBadge({ label, status }: { label: string; status: QueueJobStatus }) {
  return (
    <span className={styles["statusBadge"]} data-status={status}>
      {label}
    </span>
  );
}

function JobDetail({
  job,
  labels,
  locale,
}: {
  job: QueueJob;
  labels: JobDetailLabels;
  locale: string;
}) {
  return (
    <div className={styles["jobDetail"]}>
      <dl className={styles["detailGrid"]}>
        <div>
          <dt>{labels.id}</dt>
          <dd>
            <code>{job.id}</code>
          </dd>
        </div>
        <div>
          <dt>{labels.queue}</dt>
          <dd>{job.queue}</dd>
        </div>
        <div>
          <dt>{labels.priority}</dt>
          <dd>{job.priority.toLocaleString(locale)}</dd>
        </div>
        <div>
          <dt>{labels.retries}</dt>
          <dd>
            {job.retries.toLocaleString(locale)} / {job.maxRetries.toLocaleString(locale)}
          </dd>
        </div>
        <div>
          <dt>{labels.runAfter}</dt>
          <dd>{formatQueueDateTime(job.runAfter, locale)}</dd>
        </div>
        <div>
          <dt>{labels.ranAt}</dt>
          <dd>{formatQueueDateTime(job.ranAt, locale) || labels.notRun}</dd>
        </div>
      </dl>

      <div className={styles["detailBlock"]}>
        <h4>{labels.payload}</h4>
        <pre>{prettifyQueuePayload(job.payload)}</pre>
      </div>

      {job.error ? (
        <div className={styles["detailBlock"]}>
          <h4>{labels.error}</h4>
          <pre>{job.error}</pre>
        </div>
      ) : null}
    </div>
  );
}

export function QueueJobsSection() {
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  const [selectedQueues, setSelectedQueues] = useState<string[]>([]);
  const [selectedStatuses, setSelectedStatuses] = useState<QueueJobStatus[]>([]);
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(DEFAULT_QUEUE_PAGE_SIZE);
  const [order, setOrder] = useState<QueueOrderSelection>(DEFAULT_QUEUE_ORDER);
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null);
  const filters = useMemo<QueueFilterSelection>(
    () => ({
      queues: selectedQueues,
      statuses: selectedStatuses,
    }),
    [selectedQueues, selectedStatuses],
  );
  const {
    data: jobsData,
    dataUpdatedAt,
    error: jobsError,
    isError: isJobsError,
    isFetching: isJobsFetching,
    isPending: isJobsPending,
    refetch: refetchJobs,
  } = useQuery({
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) =>
      execute(
        QueueJobsDocument,
        createQueueJobsVariables({
          filters,
          limit,
          order,
          page,
        }),
        signal,
      ),
    queryKey: [
      "queueJobs",
      page,
      limit,
      order.field,
      order.descending,
      selectedQueues,
      selectedStatuses,
    ],
  });
  const result = jobsData?.queue.jobs;
  const jobs = result?.items ?? EMPTY_JOBS;
  const now = useMemo(
    () => (dataUpdatedAt > 0 ? new Date(dataUpdatedAt) : new Date()),
    [dataUpdatedAt],
  );
  const queueOptions = useMemo(
    () => mergeQueueOptions(result?.aggregations.queue ?? EMPTY_QUEUE_AGGS, selectedQueues),
    [result?.aggregations.queue, selectedQueues],
  );
  const statusOptions = useMemo(
    () =>
      mergeStatusOptions(
        result?.aggregations.status ?? EMPTY_STATUS_AGGS,
        selectedStatuses,
        (status) => t(`queue.status.${status}`),
      ),
    [result?.aggregations.status, selectedStatuses, t],
  );
  const totalCount = result?.totalCount ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / limit));
  const hasNextPage = result?.hasNextPage ?? page < totalPages;
  const tableLabels = {
    createdAt: t("queue.jobs.createdAt"),
    error: t("queue.jobs.error"),
    expand: t("queue.jobs.expand"),
    id: t("queue.jobs.id"),
    notRun: t("queue.jobs.notRun"),
    payload: t("queue.jobs.payload"),
    priority: t("queue.jobs.priority"),
    queue: t("queue.jobs.queue"),
    ranAt: t("queue.jobs.ranAt"),
    retries: t("queue.jobs.retries"),
    runAfter: t("queue.jobs.runAfter"),
    status: t("queue.jobs.status"),
  };

  useEffect(() => {
    if (expandedJobId && !jobs.some((job) => job.id === expandedJobId)) {
      setExpandedJobId(null);
    }
  }, [expandedJobId, jobs]);

  function handleOrderFieldChange(event: ChangeEvent<HTMLSelectElement>) {
    const nextField = event.target.value;

    if (!isQueueOrderField(nextField)) {
      return;
    }

    const option = QUEUE_ORDER_OPTIONS.find((candidate) => candidate.field === nextField);
    setOrder({
      descending: option?.defaultDescending ?? true,
      field: nextField,
    });
    setPage(1);
  }

  function handleLimitChange(event: ChangeEvent<HTMLSelectElement>) {
    setLimit(Number.parseInt(event.target.value, 10));
    setPage(1);
  }

  function toggleExpandedJob(id: string) {
    setExpandedJobId((current) => (current === id ? null : id));
  }

  return (
    <section className={styles["section"]} id="queue-jobs">
      <div className={styles["sectionHeader"]}>
        <div>
          <h2>{t("queue.jobs.title")}</h2>
          <p>{t("queue.jobs.body")}</p>
        </div>
        <span className={styles["sectionKicker"]}>
          {t("queue.jobs.count", { count: totalCount })}
        </span>
      </div>

      <div className={styles["jobsControls"]}>
        <FacetChipGroup
          allLabel={t("queue.facets.allQueues")}
          legend={t("queue.facets.queue")}
          onClear={() => {
            setSelectedQueues([]);
            setPage(1);
          }}
          onToggle={(queue) => {
            setSelectedQueues((current) => toggleQueueFilter(current, queue));
            setPage(1);
          }}
          options={queueOptions}
          selected={selectedQueues}
        />
        <FacetChipGroup
          allLabel={t("queue.facets.allStatuses")}
          legend={t("queue.facets.status")}
          onClear={() => {
            setSelectedStatuses([]);
            setPage(1);
          }}
          onToggle={(status) => {
            setSelectedStatuses((current) => toggleStatusFilter(current, status));
            setPage(1);
          }}
          options={statusOptions}
          selected={selectedStatuses}
        />
        <div className={styles["jobsToolbar"]}>
          <label>
            <span>{t("queue.jobs.orderBy")}</span>
            <select onChange={handleOrderFieldChange} value={order.field}>
              {QUEUE_ORDER_OPTIONS.map((option) => (
                <option key={option.field} value={option.field}>
                  {t(`queue.order.${option.field}`)}
                </option>
              ))}
            </select>
          </label>
          <button
            className={styles["secondaryButton"]}
            onClick={() =>
              setOrder((current) => ({
                ...current,
                descending: !current.descending,
              }))
            }
            type="button"
          >
            {order.descending ? t("queue.jobs.descending") : t("queue.jobs.ascending")}
          </button>
          <button
            className={styles["secondaryButton"]}
            disabled={isJobsFetching}
            onClick={() => {
              void refetchJobs();
            }}
            type="button"
          >
            {t("queue.jobs.refresh")}
          </button>
        </div>
      </div>

      <div className={styles["busyBar"]} data-active={isJobsFetching} />

      {isJobsError ? <QueryError error={jobsError} onRetry={() => void refetchJobs()} /> : null}

      {isJobsPending ? (
        <ListSkeleton ariaLabel={t("queue.jobs.loading")} rows={6} />
      ) : jobs.length === 0 ? (
        <div className={styles["emptyState"]} role="status">
          <h3>{t("queue.jobs.emptyTitle")}</h3>
          <p>{t("queue.jobs.emptyBody")}</p>
        </div>
      ) : (
        <div className={styles["tableScroll"]}>
          <table className={styles["jobsTable"]}>
            <thead>
              <tr>
                <th>{tableLabels.id}</th>
                <th>{tableLabels.queue}</th>
                <th>{tableLabels.priority}</th>
                <th>{tableLabels.status}</th>
                <th>{tableLabels.error}</th>
                <th>{tableLabels.createdAt}</th>
                <th>{tableLabels.ranAt}</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const expanded = expandedJobId === job.id;
                const statusLabel = t(`queue.status.${job.status}`);

                return (
                  <Fragment key={job.id}>
                    <tr
                      className={styles["summaryRow"]}
                      data-expanded={expanded}
                      onClick={() => toggleExpandedJob(job.id)}
                    >
                      <td className={styles["idCell"]} data-label={tableLabels.id}>
                        <button
                          aria-expanded={expanded}
                          aria-label={
                            expanded ? t("queue.jobs.collapseJob") : t("queue.jobs.expandJob")
                          }
                          className={styles["expandButton"]}
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleExpandedJob(job.id);
                          }}
                          type="button"
                        >
                          {expanded ? "-" : "+"}
                        </button>
                        <code>{job.id}</code>
                      </td>
                      <td className={styles["queueCell"]} data-label={tableLabels.queue}>
                        {job.queue}
                      </td>
                      <td className={styles["priorityCell"]} data-label={tableLabels.priority}>
                        {job.priority.toLocaleString(locale)}
                      </td>
                      <td className={styles["statusCell"]} data-label={tableLabels.status}>
                        <StatusBadge label={statusLabel} status={job.status} />
                      </td>
                      <td className={styles["errorCell"]} data-label={tableLabels.error}>
                        {job.error ? <span title={job.error}>{job.error}</span> : null}
                      </td>
                      <td
                        className={styles["timeCell"]}
                        data-label={tableLabels.createdAt}
                        title={formatQueueDateTime(job.createdAt, locale)}
                      >
                        {formatQueueRelativeTime(job.createdAt, now, locale)}
                      </td>
                      <td
                        className={styles["timeCell"]}
                        data-label={tableLabels.ranAt}
                        title={formatQueueDateTime(job.ranAt, locale)}
                      >
                        {formatQueueRelativeTime(job.ranAt, now, locale)}
                      </td>
                    </tr>
                    {expanded ? (
                      <tr className={styles["detailRow"]}>
                        <td colSpan={7}>
                          <JobDetail job={job} labels={tableLabels} locale={locale} />
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className={styles["pagination"]}>
        <label>
          <span>{t("queue.jobs.pageSize")}</span>
          <select onChange={handleLimitChange} value={limit}>
            {QUEUE_PAGE_SIZES.map((pageSize) => (
              <option key={pageSize} value={pageSize}>
                {pageSize}
              </option>
            ))}
          </select>
        </label>
        <div className={styles["paginationButtons"]}>
          <button
            className={styles["secondaryButton"]}
            disabled={page <= 1}
            onClick={() => setPage((current) => Math.max(1, current - 1))}
            type="button"
          >
            {t("queue.jobs.previous")}
          </button>
          <span>
            {t("queue.jobs.pageStatus", {
              page,
              totalPages,
            })}
          </span>
          <button
            className={styles["secondaryButton"]}
            disabled={!hasNextPage}
            onClick={() => setPage((current) => current + 1)}
            type="button"
          >
            {t("queue.jobs.next")}
          </button>
        </div>
      </div>
    </section>
  );
}
