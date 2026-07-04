import { lazy, Suspense, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { MetricsControls } from "../metrics/MetricsControls";
import {
  normalizeQueueMetrics,
  type MetricAutoRefreshInterval,
  type MetricTimeframe,
} from "../metrics/normalize";
import type { MetricsBucketDuration } from "../graphql/generated/graphql";

import styles from "./DashboardPage.module.css";

const TimelineChart = lazy(() =>
  import("../metrics/charts").then((module) => ({
    default: module.TimelineChart,
  })),
);
const TotalsChart = lazy(() =>
  import("../metrics/charts").then((module) => ({
    default: module.TotalsChart,
  })),
);

export default function DashboardPage() {
  const { t } = useTranslation();
  const [timeframe, setTimeframe] = useState<MetricTimeframe>("hours_1");
  const [bucketDuration, setBucketDuration] = useState<MetricsBucketDuration>("hour");
  const [autoRefresh, setAutoRefresh] = useState<MetricAutoRefreshInterval>("off");
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date>();
  const normalizedMetrics = useMemo(
    () =>
      normalizeQueueMetrics([], {
        duration: bucketDuration,
        now: lastUpdatedAt ?? new Date(),
        timeframe,
      }),
    [bucketDuration, lastUpdatedAt, timeframe],
  );

  return (
    <section className={styles["root"]}>
      <div className={styles["header"]}>
        <h1>{t("dashboard.title")}</h1>
        <p>{t("dashboard.body")}</p>
      </div>
      <MetricsControls
        autoRefresh={autoRefresh}
        bucketDuration={bucketDuration}
        lastUpdatedAt={lastUpdatedAt}
        onAutoRefreshChange={setAutoRefresh}
        onBucketDurationChange={setBucketDuration}
        onRefresh={() => setLastUpdatedAt(new Date())}
        onTimeframeChange={setTimeframe}
        timeframe={timeframe}
      />
      <Suspense
        fallback={<div className={styles["chartFallback"]}>{t("metrics.controls.loading")}</div>}
      >
        <div className={styles["charts"]}>
          <TimelineChart
            latencySeries={normalizedMetrics.latencySeries}
            points={normalizedMetrics.points}
            series={normalizedMetrics.eventSeries}
          />
          <TotalsChart totals={normalizedMetrics.totals.byQueue} />
        </div>
      </Suspense>
    </section>
  );
}
