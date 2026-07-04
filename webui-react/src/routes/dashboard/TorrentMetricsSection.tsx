import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { lazy, Suspense, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { execute } from "../../graphql/client";
import {
  TorrentMetricsDocument,
  type MetricsBucketDuration,
  type TorrentMetricsQueryVariables,
} from "../../graphql/generated/graphql";
import { MetricsControls } from "../../metrics/MetricsControls";
import {
  metricAutoRefreshSeconds,
  metricTimeframeSeconds,
  normalizeTorrentMetrics,
  type MetricAutoRefreshInterval,
  type MetricBucketMultiplier,
  type MetricTimeframe,
  type RawTorrentMetricsBucket,
  type TorrentMetricEvent,
} from "../../metrics/normalize";
import styles from "../DashboardPage.module.css";

const TimelineChart = lazy(() =>
  import("../../metrics/charts").then((module) => ({
    default: module.TimelineChart,
  })),
);

const EMPTY_TORRENT_METRIC_BUCKETS: RawTorrentMetricsBucket[] = [];
const TORRENT_METRIC_TIMEFRAMES = [
  "minutes_15",
  "minutes_30",
  "hours_1",
  "hours_6",
  "hours_12",
  "days_1",
  "weeks_1",
] as const satisfies readonly MetricTimeframe[];

function createTorrentMetricsVariables(
  bucketDuration: MetricsBucketDuration,
  timeframe: MetricTimeframe,
  source: string | null,
): TorrentMetricsQueryVariables {
  const timeframeSeconds = metricTimeframeSeconds[timeframe];

  return {
    input: {
      bucketDuration,
      sources: source ? [source] : undefined,
      startTime: Number.isFinite(timeframeSeconds)
        ? new Date(Date.now() - timeframeSeconds * 1000).toISOString()
        : undefined,
    },
  };
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

export function TorrentMetricsSection() {
  const { t } = useTranslation();
  const [timeframe, setTimeframe] = useState<MetricTimeframe>("hours_1");
  const [bucketDuration, setBucketDuration] = useState<MetricsBucketDuration>("minute");
  const [bucketMultiplier, setBucketMultiplier] = useState<MetricBucketMultiplier>(1);
  const [selectedSource, setSelectedSource] = useState<string | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<TorrentMetricEvent | null>(null);
  const [autoRefresh, setAutoRefresh] = useState<MetricAutoRefreshInterval>("seconds_10");
  const autoRefreshSeconds = metricAutoRefreshSeconds[autoRefresh];
  const {
    data: metricsData,
    dataUpdatedAt,
    error: metricsError,
    isError: isMetricsError,
    isFetching: isMetricsFetching,
    isPending: isMetricsPending,
    refetch: refetchMetrics,
  } = useQuery({
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) =>
      execute(
        TorrentMetricsDocument,
        createTorrentMetricsVariables(bucketDuration, timeframe, selectedSource),
        signal,
      ),
    queryKey: ["torrentMetrics", bucketDuration, timeframe, selectedSource],
    refetchInterval: autoRefreshSeconds ? autoRefreshSeconds * 1000 : false,
  });
  const metricsUpdatedAt = dataUpdatedAt > 0 ? dataUpdatedAt : undefined;
  const lastUpdatedAt = metricsUpdatedAt ? new Date(metricsUpdatedAt) : undefined;
  const sourceOptions = useMemo(
    () =>
      (metricsData?.torrent.listSources.sources ?? []).map((source) => ({
        label: source.name,
        value: source.key,
      })),
    [metricsData],
  );
  const eventOptions = useMemo(
    () =>
      [
        {
          defaultLabel: "Created",
          value: "created",
        },
        {
          defaultLabel: "Updated",
          value: "updated",
        },
      ].map((event) => ({
        label: t(`metrics.events.${event.value}`, event.defaultLabel),
        value: event.value,
      })),
    [t],
  );
  const rawBuckets = metricsData?.torrent.metrics.buckets ?? EMPTY_TORRENT_METRIC_BUCKETS;
  const normalizedMetrics = useMemo(
    () =>
      normalizeTorrentMetrics(rawBuckets, {
        duration: bucketDuration,
        event: selectedEvent,
        multiplier: bucketMultiplier,
        now: metricsUpdatedAt ?? new Date(),
        source: selectedSource,
        timeframe,
      }),
    [
      bucketDuration,
      bucketMultiplier,
      metricsUpdatedAt,
      rawBuckets,
      selectedEvent,
      selectedSource,
      timeframe,
    ],
  );

  return (
    <section aria-labelledby="dashboard-torrent-metrics-title" className={styles["metricsPanel"]}>
      <div className={styles["sectionHeader"]}>
        <div>
          <h2 id="dashboard-torrent-metrics-title">
            {t("dash.torrentMetrics.title", "Torrent throughput")}
          </h2>
          <p>
            {t("dash.torrentMetrics.eventsInRange", "{{count}} events in range", {
              count: normalizedMetrics.total,
            })}
          </p>
        </div>
      </div>

      <MetricsControls
        autoRefresh={autoRefresh}
        bucketDuration={bucketDuration}
        bucketMultiplier={bucketMultiplier}
        bucketMultiplierPlaceholder={normalizedMetrics.bucketParams.multiplier}
        disabled={isMetricsPending && !metricsData}
        eventFilter={{
          allLabel: t("metrics.controls.allEvents", "All events"),
          label: t("metrics.controls.event", "Event"),
          onChange: (value) => setSelectedEvent(value as TorrentMetricEvent | null),
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
          allLabel: t("metrics.controls.allSources", "All sources"),
          label: t("metrics.controls.source", "Source"),
          onChange: setSelectedSource,
          options: sourceOptions,
          value: selectedSource,
        }}
        timeframe={timeframe}
        timeframes={TORRENT_METRIC_TIMEFRAMES}
      />

      {isMetricsError ? (
        <div className={styles["inlineError"]} role="alert">
          <strong>{t("dash.torrentMetrics.errorTitle", "Torrent metrics failed")}</strong>
          <span>{getErrorMessage(metricsError) ?? t("dash.errorBody", "Try again.")}</span>
          <button
            onClick={() => {
              void refetchMetrics();
            }}
            type="button"
          >
            {t("dash.retry", "Retry")}
          </button>
        </div>
      ) : null}

      <Suspense
        fallback={
          <div className={styles["chartFallback"]}>
            {t("dash.torrentMetrics.loadingCharts", "Loading charts")}
          </div>
        }
      >
        <TimelineChart points={normalizedMetrics.points} series={normalizedMetrics.eventSeries} />
      </Suspense>
    </section>
  );
}
