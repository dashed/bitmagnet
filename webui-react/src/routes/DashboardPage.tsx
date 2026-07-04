import type { ReactNode } from "react";
import { lazy, Suspense, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { execute } from "../graphql/client";
import {
  HealthCheckDocument,
  QueueJobsDocument,
  QueueMetricsDocument,
  TorrentContentSearchDocument,
} from "../graphql/generated/graphql";
import type {
  HealthStatus,
  MetricsBucketDuration,
  QueueJobsQuery,
  QueueJobsQueryVariables,
  QueueJobStatus,
  TorrentContentSearchQueryVariables,
} from "../graphql/generated/graphql";
import { MetricsControls } from "../metrics/MetricsControls";
import {
  metricAutoRefreshSeconds,
  metricTimeframeSeconds,
  normalizeQueueMetrics,
  queueMetricStatuses,
  type MetricAutoRefreshInterval,
  type MetricBucketMultiplier,
  type MetricTimeframe,
  type QueueMetricEvent,
  type RawQueueMetricsBucket,
} from "../metrics/normalize";
import { formatIntEstimate } from "../utils/intEstimate";
import { TorrentMetricsSection } from "./dashboard/TorrentMetricsSection";
import { QUEUE_NAMES } from "./queue/constants";

import styles from "./DashboardPage.module.css";

const EMPTY_METRIC_BUCKETS: RawQueueMetricsBucket[] = [];

const QUEUE_STATUS_LABELS: Record<QueueJobStatus, string> = {
  failed: "Failed",
  pending: "Pending",
  processed: "Processed",
  retry: "Retry",
};

const HEALTH_STATUS_LABELS: Record<HealthStatus, string> = {
  down: "Down",
  inactive: "Inactive",
  unknown: "Unknown",
  up: "Up",
};

const TORRENT_TOTAL_VARIABLES: TorrentContentSearchQueryVariables = {
  cached: true,
  hasNextPage: false,
  limit: 1,
  page: 1,
  totalCount: true,
};

const QUEUE_JOB_SUMMARY_INPUT: QueueJobsQueryVariables["input"] = {
  facets: {
    queue: {
      aggregate: true,
    },
    status: {
      aggregate: true,
    },
  },
  hasNextPage: false,
  limit: 1,
  orderBy: [
    {
      descending: true,
      field: "created_at",
    },
  ],
  page: 1,
  totalCount: true,
};

const TimelineChart = lazy(async () => {
  const module = await import("../metrics/charts");

  return {
    default: module.TimelineChart,
  };
});

type CardTone = "danger" | "neutral" | "success" | "warning";

type SummaryCardProps = {
  busy?: boolean;
  children?: ReactNode;
  meta?: string;
  title: string;
  tone?: CardTone;
  value: string;
};

type StatusCount = {
  count: number;
  status: QueueJobStatus;
};

function formatCount(value: number, locale: string) {
  return value.toLocaleString(locale);
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return null;
}

function getQueueStatusCounts(
  aggregations: QueueJobsQuery["queue"]["jobs"]["aggregations"] | undefined,
): StatusCount[] {
  const counts = new Map(
    (aggregations?.status ?? []).map((aggregation) => [aggregation.value, aggregation.count]),
  );

  return queueMetricStatuses.map((status) => ({
    count: counts.get(status) ?? 0,
    status,
  }));
}

function getHealthTone(status: HealthStatus | undefined): CardTone {
  switch (status) {
    case "up":
      return "success";
    case "down":
      return "danger";
    case "inactive":
    case "unknown":
      return "warning";
    default:
      return "neutral";
  }
}

function getStatusTone(status: QueueJobStatus): CardTone {
  switch (status) {
    case "processed":
      return "success";
    case "failed":
      return "danger";
    case "retry":
      return "warning";
    case "pending":
      return "neutral";
  }
}

function createMetricsVariables(
  timeframe: MetricTimeframe,
  bucketDuration: MetricsBucketDuration,
  queue: string | null,
) {
  const timeframeSeconds = metricTimeframeSeconds[timeframe];

  return {
    input: {
      bucketDuration,
      queues: queue ? [queue] : undefined,
      startTime: Number.isFinite(timeframeSeconds)
        ? new Date(Date.now() - timeframeSeconds * 1000).toISOString()
        : undefined,
    },
  };
}

function SummaryCard({
  busy = false,
  children,
  meta,
  title,
  tone = "neutral",
  value,
}: SummaryCardProps) {
  return (
    <article aria-busy={busy || undefined} className={styles["summaryCard"]} data-tone={tone}>
      <div className={styles["summaryTop"]}>
        <h2>{title}</h2>
        <span aria-hidden="true" className={styles["toneDot"]} />
      </div>
      <strong className={styles["summaryValue"]}>{value}</strong>
      {meta ? <p className={styles["summaryMeta"]}>{meta}</p> : null}
      {children}
    </article>
  );
}

export default function DashboardPage() {
  const { i18n, t } = useTranslation();
  const [timeframe, setTimeframe] = useState<MetricTimeframe>("hours_1");
  const [bucketDuration, setBucketDuration] = useState<MetricsBucketDuration>("hour");
  const [bucketMultiplier, setBucketMultiplier] = useState<MetricBucketMultiplier>("AUTO");
  const [selectedQueue, setSelectedQueue] = useState<string | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<QueueMetricEvent | null>(null);
  const [autoRefresh, setAutoRefresh] = useState<MetricAutoRefreshInterval>("off");
  const locale = i18n.language;
  const loadingLabel = t("dash.loading", "Loading");
  const unavailableLabel = t("dash.unavailable", "Unavailable");

  const {
    data: torrentTotalData,
    isError: isTorrentTotalError,
    isFetching: isTorrentTotalFetching,
    isPending: isTorrentTotalPending,
  } = useQuery({
    queryFn: ({ signal }) => execute(TorrentContentSearchDocument, TORRENT_TOTAL_VARIABLES, signal),
    queryKey: ["dashboard", "torrentTotal"],
  });
  const {
    data: queueJobsData,
    isError: isQueueJobsError,
    isFetching: isQueueJobsFetching,
    isPending: isQueueJobsPending,
  } = useQuery({
    queryFn: ({ signal }) => execute(QueueJobsDocument, { input: QUEUE_JOB_SUMMARY_INPUT }, signal),
    queryKey: ["dashboard", "queueJobsSummary"],
  });
  const {
    data: healthData,
    isError: isHealthError,
    isFetching: isHealthFetching,
    isPending: isHealthPending,
  } = useQuery({
    queryFn: ({ signal }) => execute(HealthCheckDocument, {}, signal),
    queryKey: ["dashboard", "health"],
  });
  const metricsRefreshSeconds = metricAutoRefreshSeconds[autoRefresh];
  const {
    data: queueMetricsData,
    dataUpdatedAt: queueMetricsDataUpdatedAt,
    error: queueMetricsError,
    isError: isQueueMetricsError,
    isFetching: isQueueMetricsFetching,
    isPending: isQueueMetricsPending,
    refetch: refetchQueueMetrics,
  } = useQuery({
    queryFn: ({ signal }) =>
      execute(
        QueueMetricsDocument,
        createMetricsVariables(timeframe, bucketDuration, selectedQueue),
        signal,
      ),
    queryKey: ["dashboard", "queueMetrics", timeframe, bucketDuration, selectedQueue],
    refetchInterval: metricsRefreshSeconds ? metricsRefreshSeconds * 1000 : false,
  });

  const metricsUpdatedAt = queueMetricsDataUpdatedAt > 0 ? queueMetricsDataUpdatedAt : undefined;
  const lastUpdatedAt = metricsUpdatedAt ? new Date(metricsUpdatedAt) : undefined;
  const queueOptions = useMemo(
    () =>
      QUEUE_NAMES.map((queue) => ({
        label: queue,
        value: queue,
      })),
    [],
  );
  const eventOptions = useMemo(
    () =>
      [
        {
          defaultLabel: "Created",
          value: "created",
        },
        {
          defaultLabel: "Processed",
          value: "processed",
        },
        {
          defaultLabel: "Failed",
          value: "failed",
        },
      ].map((event) => ({
        label: t(`metrics.events.${event.value}`, event.defaultLabel),
        value: event.value,
      })),
    [t],
  );
  const normalizedMetrics = useMemo(
    () =>
      normalizeQueueMetrics(queueMetricsData?.queue.metrics.buckets ?? EMPTY_METRIC_BUCKETS, {
        duration: bucketDuration,
        event: selectedEvent,
        multiplier: bucketMultiplier,
        now: metricsUpdatedAt ?? new Date(),
        queues: selectedQueue ? [selectedQueue] : undefined,
        timeframe,
      }),
    [
      bucketDuration,
      bucketMultiplier,
      metricsUpdatedAt,
      queueMetricsData,
      selectedEvent,
      selectedQueue,
      timeframe,
    ],
  );
  const searchResult = torrentTotalData?.torrentContent.search;
  const torrentTotalValue = isTorrentTotalPending
    ? loadingLabel
    : isTorrentTotalError || !searchResult
      ? unavailableLabel
      : formatIntEstimate(searchResult.totalCount, searchResult.totalCountIsEstimate, 2, locale);
  const queueJobs = queueJobsData?.queue.jobs;
  const queueStatusCounts = getQueueStatusCounts(queueJobs?.aggregations);
  const queueJobsValue = isQueueJobsPending
    ? loadingLabel
    : isQueueJobsError || !queueJobs
      ? unavailableLabel
      : formatCount(queueJobs.totalCount, locale);
  const health = healthData?.health;
  const workers = healthData?.workers.listAll.workers ?? [];
  const startedWorkers = workers.filter((worker) => worker.started).length;
  const healthValue = isHealthPending
    ? loadingLabel
    : isHealthError || !health
      ? unavailableLabel
      : t(`dash.health.status.${health.status}`, HEALTH_STATUS_LABELS[health.status]);
  const metricEventCount = normalizedMetrics.totals.total;

  return (
    <section aria-labelledby="dashboard-title" className={styles["root"]}>
      <div className={styles["header"]}>
        <span className={styles["eyebrow"]}>{t("dash.eyebrow", "Operations")}</span>
        <h1 id="dashboard-title">{t("dash.title", "Dashboard")}</h1>
        <p>{t("dash.body", "At-a-glance torrent, queue, and service health status.")}</p>
      </div>

      <div className={styles["summaryGrid"]}>
        <SummaryCard
          busy={isTorrentTotalFetching}
          meta={
            searchResult?.totalCountIsEstimate
              ? t("dash.torrents.estimateMeta", "Estimated indexed torrent records")
              : t("dash.torrents.meta", "Indexed torrent records")
          }
          title={t("dash.torrents.title", "Torrents")}
          value={torrentTotalValue}
        />

        <SummaryCard
          busy={isQueueJobsFetching}
          meta={t("dash.queue.meta", "Jobs across all queues")}
          title={t("dash.queue.title", "Queue jobs")}
          value={queueJobsValue}
        >
          <ul
            aria-label={t("dash.queue.statusesLabel", "Queue status counts")}
            className={styles["statusList"]}
          >
            {queueStatusCounts.map(({ count, status }) => (
              <li className={styles["statusRow"]} data-tone={getStatusTone(status)} key={status}>
                <span>{t(`dash.queue.status.${status}`, QUEUE_STATUS_LABELS[status])}</span>
                <strong>{formatCount(count, locale)}</strong>
              </li>
            ))}
          </ul>
        </SummaryCard>

        <SummaryCard
          busy={isHealthFetching}
          meta={
            health
              ? `${formatCount(health.checks.length, locale)} ${t(
                  "dash.health.checksLabel",
                  "checks",
                )}, ${formatCount(startedWorkers, locale)}/${formatCount(workers.length, locale)} ${t(
                  "dash.health.workersStartedLabel",
                  "workers started",
                )}`
              : t("dash.health.meta", "Health checks and worker status")
          }
          title={t("dash.health.title", "Health")}
          tone={getHealthTone(health?.status)}
          value={healthValue}
        />
      </div>

      <section aria-labelledby="dashboard-metrics-title" className={styles["metricsPanel"]}>
        <div className={styles["sectionHeader"]}>
          <div>
            <h2 id="dashboard-metrics-title">{t("dash.metrics.title", "Queue throughput")}</h2>
            <p>
              {formatCount(metricEventCount, locale)}{" "}
              {t("dash.metrics.eventsInRange", "events in range")}
            </p>
          </div>
        </div>

        <MetricsControls
          autoRefresh={autoRefresh}
          bucketDuration={bucketDuration}
          bucketMultiplier={bucketMultiplier}
          bucketMultiplierPlaceholder={normalizedMetrics.bucketParams.multiplier}
          disabled={isQueueMetricsPending && !queueMetricsData}
          eventFilter={{
            allLabel: t("metrics.controls.allEvents", "All events"),
            label: t("metrics.controls.event", "Event"),
            onChange: (value) => setSelectedEvent(value as QueueMetricEvent | null),
            options: eventOptions,
            value: selectedEvent,
          }}
          lastUpdatedAt={lastUpdatedAt}
          loading={isQueueMetricsFetching}
          onAutoRefreshChange={setAutoRefresh}
          onBucketDurationChange={(value) => {
            setBucketDuration(value);
            setBucketMultiplier("AUTO");
          }}
          onBucketMultiplierChange={setBucketMultiplier}
          onRefresh={() => {
            void refetchQueueMetrics();
          }}
          onTimeframeChange={setTimeframe}
          scopeFilter={{
            allLabel: t("metrics.controls.allQueues", "All queues"),
            label: t("metrics.controls.queue", "Queue"),
            onChange: setSelectedQueue,
            options: queueOptions,
            value: selectedQueue,
          }}
          timeframe={timeframe}
        />

        {isQueueMetricsError ? (
          <div className={styles["inlineError"]} role="alert">
            <strong>{t("dash.metrics.errorTitle", "Queue metrics failed")}</strong>
            <span>{getErrorMessage(queueMetricsError) ?? t("dash.errorBody", "Try again.")}</span>
            <button
              onClick={() => {
                void refetchQueueMetrics();
              }}
              type="button"
            >
              {t("dash.retry", "Retry")}
            </button>
          </div>
        ) : null}

        <Suspense fallback={<div className={styles["chartFallback"]}>{loadingLabel}</div>}>
          <TimelineChart
            latencySeries={normalizedMetrics.latencySeries}
            points={normalizedMetrics.points}
            series={normalizedMetrics.eventSeries}
          />
        </Suspense>
      </section>

      <TorrentMetricsSection />

      <section aria-labelledby="dashboard-links-title" className={styles["linksSection"]}>
        <div className={styles["sectionHeader"]}>
          <h2 id="dashboard-links-title">{t("dash.links.title", "Quick links")}</h2>
        </div>
        <div className={styles["linkGrid"]}>
          <Link className={styles["quickLink"]} to="/queue">
            <strong>{t("dash.links.queue.title", "Queue")}</strong>
            <span>{t("dash.links.queue.body", "Inspect queue jobs and processing state.")}</span>
          </Link>
          <Link className={styles["quickLink"]} to="/health">
            <strong>{t("dash.links.health.title", "Health")}</strong>
            <span>{t("dash.links.health.body", "Open service checks and worker status.")}</span>
          </Link>
        </div>
      </section>
    </section>
  );
}
