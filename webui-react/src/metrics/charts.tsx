import {
  Area,
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useTranslation } from "react-i18next";

import {
  queueMetricStatuses,
  type ChartPoint,
  type ChartSeries,
  type QueueTotalsPoint,
} from "./normalize";
import type { QueueJobStatus } from "../graphql/generated/graphql";

import styles from "./charts.module.css";

const seriesColors = [
  "var(--mantine-color-blue-6)",
  "var(--mantine-color-teal-6)",
  "var(--mantine-color-orange-6)",
  "var(--mantine-color-grape-6)",
  "var(--mantine-color-cyan-6)",
  "var(--mantine-color-pink-6)",
  "var(--mantine-color-indigo-6)",
  "var(--mantine-color-lime-7)",
];

const statusColors: Record<QueueJobStatus, string> = {
  failed: "var(--mantine-color-red-6)",
  pending: "var(--mantine-color-blue-6)",
  processed: "var(--mantine-color-green-6)",
  retry: "var(--mantine-color-yellow-7)",
};

export type TimelineChartProps = {
  height?: number;
  latencySeries?: readonly ChartSeries[];
  points: readonly ChartPoint[];
  series: readonly ChartSeries[];
  stacked?: boolean;
};

export type TotalsChartProps = {
  height?: number;
  statuses?: readonly QueueJobStatus[];
  totals: readonly QueueTotalsPoint[];
};

export function TimelineChart({
  height = 320,
  latencySeries = [],
  points,
  series,
  stacked = true,
}: TimelineChartProps) {
  const { t } = useTranslation();

  if (!points.length || !series.length) {
    return <div className={styles["empty"]}>{t("metrics.charts.empty")}</div>;
  }

  return (
    <div className={styles["chart"]} style={{ blockSize: height }}>
      <ResponsiveContainer height="100%" width="100%">
        <ComposedChart data={points.slice()}>
          <CartesianGrid stroke="var(--mantine-color-default-border)" strokeDasharray="3 3" />
          <XAxis dataKey="label" minTickGap={24} stroke="var(--mantine-color-dimmed)" />
          <YAxis allowDecimals={false} stroke="var(--mantine-color-dimmed)" yAxisId="count" />
          {latencySeries.length ? (
            <YAxis
              orientation="right"
              stroke="var(--mantine-color-dimmed)"
              tickFormatter={(value) => t("metrics.charts.seconds", { value })}
              yAxisId="latency"
            />
          ) : null}
          <Tooltip
            contentStyle={{
              background: "var(--mantine-color-body)",
              borderColor: "var(--mantine-color-default-border)",
              color: "var(--mantine-color-text)",
            }}
          />
          <Legend />
          {series.map((nextSeries, index) => (
            <Area
              dataKey={nextSeries.dataKey}
              fill={seriesColors[index % seriesColors.length]}
              fillOpacity={0.18}
              key={nextSeries.dataKey}
              name={nextSeries.label}
              stackId={stacked ? "count" : undefined}
              stroke={seriesColors[index % seriesColors.length]}
              type="monotone"
              yAxisId="count"
            />
          ))}
          {latencySeries.map((nextSeries, index) => (
            <Line
              connectNulls
              dataKey={nextSeries.dataKey}
              dot={false}
              key={nextSeries.dataKey}
              name={nextSeries.label}
              stroke={seriesColors[(series.length + index) % seriesColors.length]}
              strokeDasharray="5 4"
              type="monotone"
              yAxisId="latency"
            />
          ))}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

export function TotalsChart({
  height = 320,
  statuses = queueMetricStatuses,
  totals,
}: TotalsChartProps) {
  const { t } = useTranslation();

  if (!totals.length) {
    return <div className={styles["empty"]}>{t("metrics.charts.empty")}</div>;
  }

  return (
    <div className={styles["chart"]} style={{ blockSize: height }}>
      <ResponsiveContainer height="100%" width="100%">
        <BarChart data={totals.slice()} layout="vertical">
          <CartesianGrid stroke="var(--mantine-color-default-border)" strokeDasharray="3 3" />
          <XAxis allowDecimals={false} stroke="var(--mantine-color-dimmed)" type="number" />
          <YAxis dataKey="queue" stroke="var(--mantine-color-dimmed)" type="category" width={150} />
          <Tooltip
            contentStyle={{
              background: "var(--mantine-color-body)",
              borderColor: "var(--mantine-color-default-border)",
              color: "var(--mantine-color-text)",
            }}
          />
          <Legend />
          {statuses.map((status) => (
            <Bar
              dataKey={status}
              fill={statusColors[status]}
              key={status}
              name={t(`metrics.statuses.${status}`)}
              stackId="total"
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
