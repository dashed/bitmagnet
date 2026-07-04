import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { QueryError } from "../../components/QueryError";
import { execute } from "../../graphql/client";
import { QueueMetricsDocument, type MetricsBucketDuration } from "../../graphql/generated/graphql";
import { MetricsControls } from "../../metrics/MetricsControls";
import {
  metricAutoRefreshSeconds,
  normalizeQueueMetrics,
  type MetricAutoRefreshInterval,
  type MetricBucketMultiplier,
  type MetricTimeframe,
  type QueueMetricEvent,
  type RawQueueMetricsBucket,
} from "../../metrics/normalize";
import { QUEUE_NAMES } from "./constants";
import { createQueueMetricsVariables } from "./variables";
import styles from "../QueuePage.module.css";

const TimelineChart = lazy(() =>
  import("../../metrics/charts").then((module) => ({
    default: module.TimelineChart,
  })),
);
const TotalsChart = lazy(() =>
  import("../../metrics/charts").then((module) => ({
    default: module.TotalsChart,
  })),
);

const EMPTY_QUEUE_METRIC_BUCKETS: RawQueueMetricsBucket[] = [];

export function QueueMetricsSection() {
  const { t } = useTranslation();
  const [timeframe, setTimeframe] = useState<MetricTimeframe>("hours_1");
  const [bucketDuration, setBucketDuration] = useState<MetricsBucketDuration>("hour");
  const [bucketMultiplier, setBucketMultiplier] = useState<MetricBucketMultiplier>("AUTO");
  const [selectedQueue, setSelectedQueue] = useState<string | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<QueueMetricEvent | null>(null);
  const [autoRefresh, setAutoRefresh] = useState<MetricAutoRefreshInterval>("seconds_10");
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date>();
  const autoRefreshSeconds = metricAutoRefreshSeconds[autoRefresh];
  const {
    data: metricsData,
    dataUpdatedAt,
    error: metricsError,
    isError: isMetricsError,
    isFetching: isMetricsFetching,
    refetch: refetchMetrics,
  } = useQuery({
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) =>
      execute(
        QueueMetricsDocument,
        createQueueMetricsVariables(bucketDuration, timeframe, selectedQueue),
        signal,
      ),
    queryKey: ["queueMetrics", bucketDuration, timeframe, selectedQueue],
    refetchInterval: autoRefreshSeconds ? autoRefreshSeconds * 1000 : false,
  });

  useEffect(() => {
    if (dataUpdatedAt > 0) {
      setLastUpdatedAt(new Date(dataUpdatedAt));
    }
  }, [dataUpdatedAt]);

  const rawBuckets = metricsData?.queue.metrics.buckets ?? EMPTY_QUEUE_METRIC_BUCKETS;
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
        label: t(`metrics.events.${event.value}`),
        value: event.value,
      })),
    [t],
  );
  const normalizedMetrics = useMemo(
    () =>
      normalizeQueueMetrics(rawBuckets, {
        duration: bucketDuration,
        event: selectedEvent,
        multiplier: bucketMultiplier,
        now: lastUpdatedAt ?? new Date(),
        queues: selectedQueue ? [selectedQueue] : undefined,
        timeframe,
      }),
    [
      bucketDuration,
      bucketMultiplier,
      lastUpdatedAt,
      rawBuckets,
      selectedEvent,
      selectedQueue,
      timeframe,
    ],
  );

  return (
    <section className={styles["section"]} id="queue-visualize">
      <div className={styles["sectionHeader"]}>
        <div>
          <h2>{t("queue.visualize.title")}</h2>
          <p>{t("queue.visualize.body")}</p>
        </div>
        <span className={styles["sectionKicker"]}>
          {t("queue.visualize.total", {
            count: normalizedMetrics.totals.total,
          })}
        </span>
      </div>

      <MetricsControls
        autoRefresh={autoRefresh}
        bucketDuration={bucketDuration}
        bucketMultiplier={bucketMultiplier}
        bucketMultiplierPlaceholder={normalizedMetrics.bucketParams.multiplier}
        eventFilter={{
          allLabel: t("metrics.controls.allEvents"),
          label: t("metrics.controls.event"),
          onChange: (value) => setSelectedEvent(value as QueueMetricEvent | null),
          options: eventOptions,
          value: selectedEvent,
        }}
        lastUpdatedAt={lastUpdatedAt}
        loading={isMetricsFetching}
        onAutoRefreshChange={setAutoRefresh}
        onBucketDurationChange={(value) => {
          setBucketDuration(value);
          setBucketMultiplier("AUTO");
        }}
        onBucketMultiplierChange={setBucketMultiplier}
        onRefresh={() => {
          void refetchMetrics();
        }}
        onTimeframeChange={setTimeframe}
        scopeFilter={{
          allLabel: t("metrics.controls.allQueues"),
          label: t("metrics.controls.queue"),
          onChange: setSelectedQueue,
          options: queueOptions,
          value: selectedQueue,
        }}
        timeframe={timeframe}
      />

      {isMetricsError ? (
        <QueryError error={metricsError} onRetry={() => void refetchMetrics()} />
      ) : null}

      <Suspense
        fallback={
          <div className={styles["chartFallback"]} role="status">
            {t("queue.visualize.loadingCharts")}
          </div>
        }
      >
        <div className={styles["chartGrid"]}>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.eventsTitle")}</h3>
            <TimelineChart
              latencySeries={normalizedMetrics.latencySeries}
              points={normalizedMetrics.points}
              series={normalizedMetrics.eventSeries}
            />
          </article>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.statusTitle")}</h3>
            <TimelineChart
              points={normalizedMetrics.statusPoints}
              series={normalizedMetrics.statusSeries}
            />
          </article>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.totalsTitle")}</h3>
            <TotalsChart totals={normalizedMetrics.totals.byQueue} />
          </article>
        </div>
      </Suspense>
    </section>
  );
}
