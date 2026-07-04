import { Button, SegmentedControl, Select, Text } from "@mantine/core";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { formatRelativeTime } from "../utils/relativeTime";
import {
  metricAutoRefreshIntervals,
  metricBucketDurations,
  metricTimeframes,
  type MetricAutoRefreshInterval,
  type MetricTimeframe,
} from "./normalize";
import type { MetricsBucketDuration } from "../graphql/generated/graphql";

import styles from "./MetricsControls.module.css";

export type MetricsControlsProps = {
  autoRefresh: MetricAutoRefreshInterval;
  bucketDuration: MetricsBucketDuration;
  disabled?: boolean;
  lastUpdatedAt?: Date;
  loading?: boolean;
  onAutoRefreshChange: (value: MetricAutoRefreshInterval) => void;
  onBucketDurationChange: (value: MetricsBucketDuration) => void;
  onRefresh?: () => void;
  onTimeframeChange: (value: MetricTimeframe) => void;
  timeframe: MetricTimeframe;
};

export function MetricsControls({
  autoRefresh,
  bucketDuration,
  disabled = false,
  lastUpdatedAt,
  loading = false,
  onAutoRefreshChange,
  onBucketDurationChange,
  onRefresh,
  onTimeframeChange,
  timeframe,
}: MetricsControlsProps) {
  const { i18n, t } = useTranslation();
  const timeframeOptions = useMemo(
    () =>
      metricTimeframes.map((value) => ({
        label: t(`metrics.timeframes.${value}`),
        value,
      })),
    [t],
  );
  const bucketDurationOptions = useMemo(
    () =>
      metricBucketDurations.map((value) => ({
        label: t(`metrics.bucketDurations.${value}`),
        value,
      })),
    [t],
  );
  const autoRefreshOptions = useMemo(
    () =>
      metricAutoRefreshIntervals.map((value) => ({
        label: t(`metrics.autoRefresh.${value}`),
        value,
      })),
    [t],
  );
  const lastUpdated = lastUpdatedAt
    ? t("metrics.controls.lastUpdated", {
        time: formatRelativeTime(lastUpdatedAt.toISOString(), new Date(), i18n.language),
      })
    : t("metrics.controls.waiting");

  return (
    <div className={styles["root"]}>
      <Select
        allowDeselect={false}
        data={timeframeOptions}
        disabled={disabled}
        label={t("metrics.controls.timeframe")}
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
        label={t("metrics.controls.bucketDuration")}
        onChange={(value) => {
          if (value) {
            onBucketDurationChange(value as MetricsBucketDuration);
          }
        }}
        value={bucketDuration}
      />
      <SegmentedControl
        className={styles["autoRefresh"]}
        data={autoRefreshOptions}
        disabled={disabled}
        onChange={onAutoRefreshChange}
        value={autoRefresh}
      />
      <div className={styles["status"]}>
        <Text component="span" size="sm">
          {loading ? t("metrics.controls.loading") : lastUpdated}
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
          {t("metrics.controls.refresh")}
        </Button>
      ) : null}
    </div>
  );
}
