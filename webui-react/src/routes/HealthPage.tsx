import { useQuery } from "@tanstack/react-query";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../components/ListSkeleton";
import { execute } from "../graphql/client";
import { HealthCheckDocument } from "../graphql/generated/graphql";
import type { HealthCheckQuery, HealthStatus } from "../graphql/generated/graphql";
import { formatRelativeTime } from "../utils/relativeTime";

import styles from "./HealthPage.module.css";

const REFRESH_INTERVAL_MS = 30_000;

type HealthCheckItem = HealthCheckQuery["health"]["checks"][number];
type WorkerItem = HealthCheckQuery["workers"]["listAll"]["workers"][number];
type OverallTone = "degraded" | "ok";
type WorkerState = "started" | "stopped";

const HEALTH_STATUS_LABELS = {
  down: "Down",
  inactive: "Inactive",
  unknown: "Pending",
  up: "Up",
} satisfies Record<HealthStatus, string>;

const OVERALL_STATUS_LABELS = {
  degraded: "Degraded",
  ok: "OK",
} satisfies Record<OverallTone, string>;

const WORKER_STATE_LABELS = {
  started: "Started",
  stopped: "Stopped",
} satisfies Record<WorkerState, string>;

function getOverallTone(status: HealthStatus): OverallTone {
  return status === "up" ? "ok" : "degraded";
}

function getHealthStatusLabel(status: HealthStatus, t: TFunction) {
  return t(`health.statuses.${status}`, HEALTH_STATUS_LABELS[status]);
}

function getOverallStatusLabel(tone: OverallTone, t: TFunction) {
  return t(`health.overallStatuses.${tone}`, OVERALL_STATUS_LABELS[tone]);
}

function getWorkerStateLabel(state: WorkerState, t: TFunction) {
  return t(`health.workerStates.${state}`, WORKER_STATE_LABELS[state]);
}

function getFormattedTimestamp(value: string, locale: string, now: Date) {
  return {
    absolute: new Date(value).toLocaleString(locale),
    relative: formatRelativeTime(value, now, locale),
  };
}

function StatusBadge({
  children,
  status,
}: {
  children: string;
  status: HealthStatus | WorkerState;
}) {
  return (
    <span className={styles["statusBadge"]} data-status={status}>
      {children}
    </span>
  );
}

function ChecksTable({
  checks,
  locale,
  now,
  t,
}: {
  checks: HealthCheckItem[];
  locale: string;
  now: Date;
  t: TFunction;
}) {
  if (checks.length === 0) {
    return <p className={styles["emptyText"]}>{t("health.noChecks", "No checks reported.")}</p>;
  }

  const keyLabel = t("health.key", "Key");
  const statusLabel = t("health.status", "Status");
  const checkedLabel = t("health.lastChecked", "Last checked");
  const errorLabel = t("health.error", "Error");

  return (
    <div className={styles["tableScroll"]}>
      <table className={styles["checksTable"]}>
        <thead>
          <tr>
            <th scope="col">{keyLabel}</th>
            <th scope="col">{statusLabel}</th>
            <th scope="col">{checkedLabel}</th>
            <th scope="col">{errorLabel}</th>
          </tr>
        </thead>
        <tbody>
          {checks.map((check) => {
            const timestamp = getFormattedTimestamp(check.timestamp, locale, now);

            return (
              <tr key={check.key}>
                <td data-label={keyLabel}>
                  <code>{check.key}</code>
                </td>
                <td data-label={statusLabel}>
                  <StatusBadge status={check.status}>
                    {getHealthStatusLabel(check.status, t)}
                  </StatusBadge>
                </td>
                <td data-label={checkedLabel}>
                  <time dateTime={check.timestamp} title={timestamp.absolute}>
                    {timestamp.relative}
                  </time>
                </td>
                <td data-label={errorLabel}>
                  {check.error ? (
                    <span className={styles["errorText"]}>{check.error}</span>
                  ) : (
                    <span className={styles["mutedText"]}>{t("health.noError", "None")}</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function WorkersList({ t, workers }: { t: TFunction; workers: WorkerItem[] }) {
  if (workers.length === 0) {
    return <p className={styles["emptyText"]}>{t("health.noWorkers", "No workers reported.")}</p>;
  }

  return (
    <ul className={styles["workersList"]}>
      {workers.map((worker) => {
        const state: WorkerState = worker.started ? "started" : "stopped";

        return (
          <li className={styles["workerItem"]} key={worker.key}>
            <code>{worker.key}</code>
            <StatusBadge status={state}>{getWorkerStateLabel(state, t)}</StatusBadge>
          </li>
        );
      })}
    </ul>
  );
}

export default function HealthPage() {
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  const now = new Date();
  const {
    data,
    dataUpdatedAt,
    error,
    isError,
    isFetching,
    isPending,
    refetch,
  } = useQuery({
    queryFn: ({ signal }) => execute(HealthCheckDocument, {}, signal),
    queryKey: ["healthCheck"],
    refetchInterval: REFRESH_INTERVAL_MS,
  });

  if (isPending) {
    return <ListSkeleton ariaLabel={t("health.loading", "Loading health status")} rows={5} />;
  }

  if (isError) {
    const message = error instanceof Error ? error.message : t("health.errorFallback", "Unknown error");

    return (
      <section className={styles["root"]}>
        <div className={styles["header"]}>
          <h1>{t("health.title", "Health")}</h1>
          <p>{t("health.description", "Service checks and worker state.")}</p>
        </div>
        <div className={styles["errorPanel"]} role="alert">
          <h2>{t("health.loadFailed", "Health check failed")}</h2>
          <p>{message}</p>
          <button className={styles["refreshButton"]} onClick={() => void refetch()} type="button">
            {t("health.retry", "Retry")}
          </button>
        </div>
      </section>
    );
  }

  const health = data.health;
  const workers = data.workers.listAll.workers;
  const overallTone = getOverallTone(health.status);
  const overallStatusLabel = getOverallStatusLabel(overallTone, t);
  const updatedAt = dataUpdatedAt > 0 ? new Date(dataUpdatedAt).toISOString() : null;
  const updatedTimestamp = updatedAt ? getFormattedTimestamp(updatedAt, locale, now) : null;

  return (
    <section className={styles["root"]}>
      <div className={styles["header"]}>
        <div>
          <h1>{t("health.title", "Health")}</h1>
          <p>{t("health.description", "Service checks and worker state.")}</p>
        </div>
        <button
          className={styles["refreshButton"]}
          disabled={isFetching}
          onClick={() => void refetch()}
          type="button"
        >
          {isFetching ? t("health.refreshing", "Refreshing...") : t("health.refresh", "Refresh")}
        </button>
      </div>

      <div className={styles["statusBanner"]} data-tone={overallTone}>
        <div className={styles["statusMarker"]} aria-hidden="true" />
        <div className={styles["statusCopy"]}>
          <p className={styles["eyebrow"]}>{t("health.overallStatus", "Overall status")}</p>
          <h2>
            {t("health.bitmagnet_is_status", "bitmagnet is {{status}}", {
              status: overallStatusLabel,
            })}
          </h2>
          {updatedTimestamp ? (
            <p className={styles["updatedText"]}>
              {t("health.lastUpdated", "Updated {{time}}", {
                time: updatedTimestamp.relative,
              })}
            </p>
          ) : null}
        </div>
      </div>

      <section className={styles["panel"]}>
        <div className={styles["sectionHeader"]}>
          <h2>{t("health.checks", "Checks")}</h2>
        </div>
        <ChecksTable checks={health.checks} locale={locale} now={now} t={t} />
      </section>

      <section className={styles["panel"]}>
        <div className={styles["sectionHeader"]}>
          <h2>{t("health.workers", "Workers")}</h2>
        </div>
        <WorkersList t={t} workers={workers} />
      </section>
    </section>
  );
}
