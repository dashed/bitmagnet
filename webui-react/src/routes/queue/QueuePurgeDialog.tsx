import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type { QueueFilterSelection } from "./variables";
import { getErrorMessage } from "./format";
import { useDialogFocus } from "../../utils/dialogFocus";
import styles from "../QueuePage.module.css";

type QueuePurgeDialogProps = {
  error: unknown;
  filters: QueueFilterSelection;
  isPending: boolean;
  onClose: () => void;
  onConfirm: () => void;
  open: boolean;
};

export function QueuePurgeDialog({
  error,
  filters,
  isPending,
  onClose,
  onConfirm,
  open,
}: QueuePurgeDialogProps) {
  const { t } = useTranslation();
  const [acknowledged, setAcknowledged] = useState(false);
  const dialogRef = useDialogFocus(open, onClose);
  const queueScope =
    filters.queues.length > 0
      ? filters.queues.join(", ")
      : t("queue.admin.allQueues", "all queues");
  const statusScope =
    filters.statuses.length > 0
      ? filters.statuses.map((status) => t(`queue.status.${status}`, status)).join(", ")
      : t("queue.admin.allStatuses", "all statuses");
  const fullQueuePurge = filters.queues.length === 0 && filters.statuses.length === 0;

  useEffect(() => {
    if (open) {
      setAcknowledged(false);
    }
  }, [open, filters.queues, filters.statuses]);

  if (!open) {
    return null;
  }

  return (
    <div
      className={styles["dialogBackdrop"]}
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
      role="presentation"
    >
      <div
        aria-describedby="queue-purge-dialog-body"
        aria-labelledby="queue-purge-dialog-title"
        aria-modal="true"
        className={styles["dialog"]}
        ref={dialogRef}
        role="dialog"
        tabIndex={-1}
      >
        <h3 id="queue-purge-dialog-title">{t("queue.admin.dialogTitle", "Purge queue jobs")}</h3>
        <p className={styles["panelText"]} id="queue-purge-dialog-body">
          {t(
            "queue.admin.dialogBody",
            "This will delete jobs in {{queueScope}} with {{statusScope}}.",
            {
              queueScope,
              statusScope,
            },
          )}
        </p>
        {fullQueuePurge ? (
          <p className={styles["warningText"]}>
            {t(
              "queue.admin.fullPurgeWarning",
              "No queue or status scope is selected, so this will purge the entire queue table.",
            )}
          </p>
        ) : null}
        {error ? (
          <p className={styles["warningText"]} role="alert">
            {t("queue.admin.dialogError", "Purge failed: {{error}}", {
              error: getErrorMessage(error),
            })}
          </p>
        ) : null}
        <label className={styles["checkboxRow"]}>
          <input
            checked={acknowledged}
            disabled={isPending}
            onChange={(event) => setAcknowledged(event.target.checked)}
            type="checkbox"
          />
          <span>
            {t(
              "queue.admin.acknowledge",
              "I understand this permanently deletes the selected queue jobs.",
            )}
          </span>
        </label>
        <div className={styles["dialogActions"]}>
          <button
            className={styles["secondaryButton"]}
            disabled={isPending}
            onClick={onClose}
            type="button"
          >
            {t("queue.admin.cancel", "Cancel")}
          </button>
          <button
            className={styles["dangerButton"]}
            disabled={!acknowledged || isPending}
            onClick={onConfirm}
            type="button"
          >
            {isPending
              ? t("queue.admin.purging", "Purging")
              : t("queue.admin.confirmPurge", "Purge jobs")}
          </button>
        </div>
      </div>
    </div>
  );
}
