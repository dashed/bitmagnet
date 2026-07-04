import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { useToast } from "../../components/toast";
import { execute } from "../../graphql/client";
import {
  QueueEnqueueReprocessTorrentsBatchDocument,
  QueuePurgeJobsDocument,
  type QueueEnqueueReprocessTorrentsBatchInput,
  type QueueJobStatus,
  type QueuePurgeJobsInput,
} from "../../graphql/generated/graphql";
import { getErrorMessage } from "./format";
import { QUEUE_NAMES, QUEUE_STATUSES, sortQueues, sortStatuses } from "./constants";
import { createQueuePurgeInput, type QueueFilterSelection } from "./variables";
import { QueuePurgeDialog } from "./QueuePurgeDialog";
import { QueueReprocessBatchDialog } from "./QueueReprocessBatchDialog";
import styles from "../QueuePage.module.css";

function ScopeChips<TValue extends string>({
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
  options: ReadonlyArray<{ label: string; value: TValue }>;
  selected: readonly TValue[];
}) {
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
            data-active={selected.includes(option.value)}
            key={option.value}
            onClick={() => onToggle(option.value)}
            type="button"
          >
            <span>{option.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function toggleQueueScope(current: readonly string[], value: string) {
  return current.includes(value)
    ? current.filter((item) => item !== value)
    : sortQueues([...current, value]);
}

function toggleStatusScope(current: readonly QueueJobStatus[], value: QueueJobStatus) {
  return current.includes(value)
    ? current.filter((item) => item !== value)
    : sortStatuses([...current, value]);
}

export function QueueAdminSection() {
  const { t } = useTranslation();
  const notify = useToast();
  const queryClient = useQueryClient();
  const [selectedQueues, setSelectedQueues] = useState<string[]>([]);
  const [selectedStatuses, setSelectedStatuses] = useState<QueueJobStatus[]>([]);
  const [purgeDialogOpen, setPurgeDialogOpen] = useState(false);
  const [reprocessDialogOpen, setReprocessDialogOpen] = useState(false);
  const filters = useMemo<QueueFilterSelection>(
    () => ({
      queues: selectedQueues,
      statuses: selectedStatuses,
    }),
    [selectedQueues, selectedStatuses],
  );
  const purgeInput = useMemo(() => createQueuePurgeInput(filters), [filters]);
  const queueOptions = useMemo(
    () =>
      QUEUE_NAMES.map((queue) => ({
        label: queue,
        value: queue,
      })),
    [],
  );
  const statusOptions = useMemo(
    () =>
      QUEUE_STATUSES.map((status) => ({
        label: t(`queue.status.${status}`, status),
        value: status,
      })),
    [t],
  );
  const purgeMutation = useMutation({
    mutationFn: (input: QueuePurgeJobsInput) =>
      execute(QueuePurgeJobsDocument, {
        input,
      }),
    onError: (error) => {
      notify({
        message: t("queue.admin.purgeError", "Failed to purge queue jobs: {{error}}", {
          error: getErrorMessage(error),
        }),
        tone: "error",
      });
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queueJobs"] });
      void queryClient.invalidateQueries({ queryKey: ["queueMetrics"] });
      setPurgeDialogOpen(false);
      notify({
        message: t("queue.admin.purgeSuccess", "Queue jobs purged"),
      });
    },
  });
  const reprocessMutation = useMutation({
    mutationFn: (input: QueueEnqueueReprocessTorrentsBatchInput) =>
      execute(QueueEnqueueReprocessTorrentsBatchDocument, {
        input,
      }),
    onError: (error) => {
      notify({
        message: t("queue.admin.reprocessError", "Failed to enqueue jobs: {{error}}", {
          error: getErrorMessage(error),
        }),
        tone: "error",
      });
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["queueJobs"] });
      void queryClient.invalidateQueries({ queryKey: ["queueMetrics"] });
      setReprocessDialogOpen(false);
      notify({
        message: t("queue.admin.reprocessSuccess", "Torrent processing batch enqueued"),
      });
    },
  });

  function openPurgeDialog() {
    purgeMutation.reset();
    setPurgeDialogOpen(true);
  }

  function closePurgeDialog() {
    if (purgeMutation.isPending) {
      return;
    }

    purgeMutation.reset();
    setPurgeDialogOpen(false);
  }

  function openReprocessDialog() {
    reprocessMutation.reset();
    setReprocessDialogOpen(true);
  }

  function closeReprocessDialog() {
    if (reprocessMutation.isPending) {
      return;
    }

    reprocessMutation.reset();
    setReprocessDialogOpen(false);
  }

  return (
    <section className={styles["section"]} id="queue-admin">
      <div className={styles["sectionHeader"]}>
        <div>
          <h2>{t("queue.admin.title", "Admin")}</h2>
          <p>
            {t(
              "queue.admin.body",
              "Purge queue jobs by queue and status, or enqueue a scoped torrent reprocess batch.",
            )}
          </p>
        </div>
      </div>

      <div className={styles["adminPanel"]}>
        <ScopeChips
          allLabel={t("queue.facets.allQueues", "All queues")}
          legend={t("queue.admin.queueScope", "Queue scope")}
          onClear={() => setSelectedQueues([])}
          onToggle={(queue) => setSelectedQueues((current) => toggleQueueScope(current, queue))}
          options={queueOptions}
          selected={selectedQueues}
        />
        <ScopeChips
          allLabel={t("queue.facets.allStatuses", "All statuses")}
          legend={t("queue.admin.statusScope", "Status scope")}
          onClear={() => setSelectedStatuses([])}
          onToggle={(status) =>
            setSelectedStatuses((current) => toggleStatusScope(current, status))
          }
          options={statusOptions}
          selected={selectedStatuses}
        />
        <div className={styles["adminActions"]}>
          <p className={styles["warningText"]}>
            {t(
              "queue.admin.warning",
              "Purge is destructive. Review the scope in the confirmation dialog before continuing.",
            )}
          </p>
          <button
            className={styles["secondaryButton"]}
            disabled={reprocessMutation.isPending}
            onClick={openReprocessDialog}
            type="button"
          >
            {t("queue.admin.openReprocess", "Enqueue reprocess batch")}
          </button>
          <button
            className={styles["dangerButton"]}
            disabled={purgeMutation.isPending}
            onClick={openPurgeDialog}
            type="button"
          >
            {t("queue.admin.openPurge", "Purge jobs")}
          </button>
        </div>
      </div>

      <QueuePurgeDialog
        error={purgeMutation.error}
        filters={filters}
        isPending={purgeMutation.isPending}
        onClose={closePurgeDialog}
        onConfirm={() => purgeMutation.mutate(purgeInput)}
        open={purgeDialogOpen}
      />
      <QueueReprocessBatchDialog
        error={reprocessMutation.error}
        isPending={reprocessMutation.isPending}
        onClose={closeReprocessDialog}
        onConfirm={(input) => reprocessMutation.mutate(input)}
        open={reprocessDialogOpen}
      />
    </section>
  );
}
