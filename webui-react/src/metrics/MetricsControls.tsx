import { Button, SegmentedControl, Select, Text } from "@mantine/core";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { formatRelativeTime } from "../utils/relativeTime";
import {
  metricAutoRefreshIntervals,
  metricBucketDurations,
  metricTimeframes,
  type MetricAutoRefreshInterval,
  type MetricBucketMultiplier,
  type MetricTimeframe,
} from "./normalize";
import type { MetricsBucketDuration } from "../graphql/generated/graphql";

import styles from "./MetricsControls.module.css";

export type MetricsControlsProps = {
  autoRefresh: MetricAutoRefreshInterval;
  bucketDuration: MetricsBucketDuration;
  bucketMultiplier?: MetricBucketMultiplier;
  bucketMultiplierPlaceholder?: number;
  disabled?: boolean;
  eventFilter?: MetricsFilterProps;
  lastUpdatedAt?: Date;
  loading?: boolean;
  onAutoRefreshChange: (value: MetricAutoRefreshInterval) => void;
  onBucketDurationChange: (value: MetricsBucketDuration) => void;
  onBucketMultiplierChange?: (value: MetricBucketMultiplier) => void;
  onRefresh?: () => void;
  onTimeframeChange: (value: MetricTimeframe) => void;
  scopeFilter?: MetricsFilterProps;
  timeframe: MetricTimeframe;
  timeframes?: readonly MetricTimeframe[];
};

export type MetricsFilterProps = {
  allLabel: string;
  label: string;
  onChange: (value: string | null) => void;
  options: ReadonlyArray<{
    label: string;
    value: string;
  }>;
  value: string | null;
};

const metricTimeframeLabels: Record<MetricTimeframe, string> = {
  all: "All time",
  days_1: "1 day",
  hours_1: "1 hour",
  hours_6: "6 hours",
  hours_12: "12 hours",
  minutes_15: "15 minutes",
  minutes_30: "30 minutes",
  weeks_1: "1 week",
};

const metricBucketDurationLabels: Record<MetricsBucketDuration, string> = {
  day: "Days",
  hour: "Hours",
  minute: "Minutes",
};

const metricAutoRefreshLabels: Record<MetricAutoRefreshInterval, string> = {
  minutes_1: "1m",
  minutes_5: "5m",
  off: "Off",
  seconds_10: "10s",
  seconds_30: "30s",
};

export function MetricsControls({
  autoRefresh,
  bucketDuration,
  bucketMultiplier,
  bucketMultiplierPlaceholder,
  disabled = false,
  eventFilter,
  lastUpdatedAt,
  loading = false,
  onAutoRefreshChange,
  onBucketDurationChange,
  onBucketMultiplierChange,
  onRefresh,
  onTimeframeChange,
  scopeFilter,
  timeframe,
  timeframes = metricTimeframes,
}: MetricsControlsProps) {
  const { i18n, t } = useTranslation();
  const timeframeOptions = useMemo(
    () =>
      timeframes.map((value) => ({
        label: t(`metrics.timeframes.${value}`, metricTimeframeLabels[value]),
        value,
      })),
    [t, timeframes],
  );
  const bucketDurationOptions = useMemo(
    () =>
      metricBucketDurations.map((value) => ({
        label: t(`metrics.bucketDurations.${value}`, metricBucketDurationLabels[value]),
        value,
      })),
    [t],
  );
  const autoRefreshOptions = useMemo(
    () =>
      metricAutoRefreshIntervals.map((value) => ({
        label: t(`metrics.autoRefresh.${value}`, metricAutoRefreshLabels[value]),
        value,
      })),
    [t],
  );
  const scopeOptions = useMemo(
    () =>
      scopeFilter ? [{ label: scopeFilter.allLabel, value: "_all" }, ...scopeFilter.options] : [],
    [scopeFilter],
  );
  const eventOptions = useMemo(
    () =>
      eventFilter ? [{ label: eventFilter.allLabel, value: "_all" }, ...eventFilter.options] : [],
    [eventFilter],
  );
  const lastUpdated = lastUpdatedAt
    ? t("metrics.controls.lastUpdated", "Updated {{time}}", {
        time: formatRelativeTime(lastUpdatedAt.toISOString(), new Date(), i18n.language),
      })
    : t("metrics.controls.waiting", "Waiting for data");
  const effectiveBucketMultiplier =
    typeof bucketMultiplier === "number" ? bucketMultiplier : (bucketMultiplierPlaceholder ?? 1);
  const multiplierValue = typeof bucketMultiplier === "number" ? `${bucketMultiplier}` : "";

  return (
    <div className={styles["root"]}>
      <Select
        allowDeselect={false}
        data={timeframeOptions}
        disabled={disabled}
        label={t("metrics.controls.timeframe", "Timeframe")}
        onChange={(value) => {
          if (value) {
            onTimeframeChange(value);
          }
        }}
        value={timeframe}
      />
      <Select
        allowDeselect={false}
        data={bucketDurationOptions}
        disabled={disabled}
        label={t("metrics.controls.bucketDuration", "Bucket")}
        onChange={(value) => {
          if (value) {
            onBucketDurationChange(value as MetricsBucketDuration);
          }
        }}
        value={bucketDuration}
      />
      {onBucketMultiplierChange ? (
        <label className={styles["numberField"]}>
          <span>{t("metrics.controls.bucketMultiplier", "Multiplier")}</span>
          <div className={styles["numberInputRow"]}>
            <button
              disabled={disabled || effectiveBucketMultiplier <= 1}
              onClick={() => onBucketMultiplierChange(Math.max(1, effectiveBucketMultiplier - 1))}
              type="button"
            >
              -
            </button>
            <input
              disabled={disabled}
              inputMode="numeric"
              min={1}
              onChange={(event) => {
                const value = event.currentTarget.value.trim();
                onBucketMultiplierChange(
                  /^\d+$/.test(value) ? Math.max(1, Number.parseInt(value, 10)) : "AUTO",
                );
              }}
              placeholder={`${bucketMultiplierPlaceholder ?? ""}`}
              step={1}
              type="number"
              value={multiplierValue}
            />
            <button
              disabled={disabled}
              onClick={() => onBucketMultiplierChange(effectiveBucketMultiplier + 1)}
              type="button"
            >
              +
            </button>
          </div>
        </label>
      ) : null}
      {scopeFilter ? (
        <Select
          allowDeselect={false}
          data={scopeOptions}
          disabled={disabled}
          label={scopeFilter.label}
          onChange={(value) => scopeFilter.onChange(value === "_all" ? null : value)}
          value={scopeFilter.value ?? "_all"}
        />
      ) : null}
      {eventFilter ? (
        <Select
          allowDeselect={false}
          data={eventOptions}
          disabled={disabled}
          label={eventFilter.label}
          onChange={(value) => eventFilter.onChange(value === "_all" ? null : value)}
          value={eventFilter.value ?? "_all"}
        />
      ) : null}
      <SegmentedControl
        className={styles["autoRefresh"]}
        data={autoRefreshOptions}
        disabled={disabled}
        onChange={onAutoRefreshChange}
        value={autoRefresh}
      />
      <div className={styles["status"]}>
        <Text component="span" size="sm">
          {loading ? t("metrics.controls.loading", "Loading metrics") : lastUpdated}
        </Text>
      </div>
      {onRefresh ? (
        <Button
          className={styles["refresh"]}
          disabled={disabled || loading}
          onClick={onRefresh}
          type="button"
          variant="light"
        >
          {t("metrics.controls.refresh", "Refresh")}
        </Button>
      ) : null}
    </div>
  );
}
