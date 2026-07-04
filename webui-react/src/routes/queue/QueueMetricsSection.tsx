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
  type MetricTimeframe,
  type RawQueueMetricsBucket,
} from "../../metrics/normalize";
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
      execute(QueueMetricsDocument, createQueueMetricsVariables(bucketDuration, timeframe), signal),
    queryKey: ["queueMetrics", bucketDuration, timeframe],
    refetchInterval: autoRefreshSeconds ? autoRefreshSeconds * 1000 : false,
  });

  useEffect(() => {
    if (dataUpdatedAt > 0) {
      setLastUpdatedAt(new Date(dataUpdatedAt));
    }
  }, [dataUpdatedAt]);

  const rawBuckets = metricsData?.queue.metrics.buckets ?? EMPTY_QUEUE_METRIC_BUCKETS;
  const normalizedMetrics = useMemo(
    () =>
      normalizeQueueMetrics(rawBuckets, {
        duration: bucketDuration,
        now: lastUpdatedAt ?? new Date(),
        timeframe,
      }),
    [bucketDuration, lastUpdatedAt, rawBuckets, timeframe],
  );

  return (
    <section className={styles["section"]} id="queue-visualize">
      <div className={styles["sectionHeader"]}>
        <div>
          <h2>{t("queue.visualize.title", "Visualize")}</h2>
          <p>
            {t(
              "queue.visualize.body",
              "Queue throughput, job status, and latency over the selected window.",
            )}
          </p>
        </div>
        <span className={styles["sectionKicker"]}>
          {t("queue.visualize.total", "{{count}} jobs", {
            count: normalizedMetrics.totals.total,
          })}
        </span>
      </div>

      <MetricsControls
        autoRefresh={autoRefresh}
        bucketDuration={bucketDuration}
        lastUpdatedAt={lastUpdatedAt}
        loading={isMetricsFetching}
        onAutoRefreshChange={setAutoRefresh}
        onBucketDurationChange={setBucketDuration}
        onRefresh={() => {
          void refetchMetrics();
        }}
        onTimeframeChange={setTimeframe}
        timeframe={timeframe}
      />

      {isMetricsError ? <QueryError error={metricsError} onRetry={() => void refetchMetrics()} /> : null}

      <Suspense
        fallback={
          <div className={styles["chartFallback"]} role="status">
            {t("queue.visualize.loadingCharts", "Loading charts")}
          </div>
        }
      >
        <div className={styles["chartGrid"]}>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.eventsTitle", "Events and latency")}</h3>
            <TimelineChart
              latencySeries={normalizedMetrics.latencySeries}
              points={normalizedMetrics.points}
              series={normalizedMetrics.eventSeries}
            />
          </article>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.statusTitle", "Statuses")}</h3>
            <TimelineChart
              points={normalizedMetrics.statusPoints}
              series={normalizedMetrics.statusSeries}
            />
          </article>
          <article className={styles["chartPanel"]}>
            <h3>{t("queue.visualize.totalsTitle", "Totals by queue")}</h3>
            <TotalsChart totals={normalizedMetrics.totals.byQueue} />
          </article>
        </div>
      </Suspense>
    </section>
  );
}
