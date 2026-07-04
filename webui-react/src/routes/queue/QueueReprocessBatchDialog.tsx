import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type {
  ContentType,
  QueueEnqueueReprocessTorrentsBatchInput,
} from "../../graphql/generated/graphql";
import { useDialogFocus } from "../../utils/dialogFocus";
import { getErrorMessage } from "./format";
import styles from "../QueuePage.module.css";

type ContentTypeSelection = ContentType | "null";
type ContentTypeScope = "all" | ContentTypeSelection[];

type QueueReprocessBatchDialogProps = {
  error: unknown;
  isPending: boolean;
  onClose: () => void;
  onConfirm: (input: QueueEnqueueReprocessTorrentsBatchInput) => void;
  open: boolean;
};

const CONTENT_TYPE_OPTIONS: ReadonlyArray<{
  defaultLabel: string;
  labelKey: string;
  value: ContentTypeSelection;
}> = [
  {
    defaultLabel: "Movies",
    labelKey: "contentTypes.movie",
    value: "movie",
  },
  {
    defaultLabel: "TV shows",
    labelKey: "contentTypes.tv_show",
    value: "tv_show",
  },
  {
    defaultLabel: "Music",
    labelKey: "contentTypes.music",
    value: "music",
  },
  {
    defaultLabel: "Ebooks",
    labelKey: "contentTypes.ebook",
    value: "ebook",
  },
  {
    defaultLabel: "Comics",
    labelKey: "contentTypes.comic",
    value: "comic",
  },
  {
    defaultLabel: "Audiobooks",
    labelKey: "contentTypes.audiobook",
    value: "audiobook",
  },
  {
    defaultLabel: "Software",
    labelKey: "contentTypes.software",
    value: "software",
  },
  {
    defaultLabel: "Games",
    labelKey: "contentTypes.game",
    value: "game",
  },
  {
    defaultLabel: "XXX",
    labelKey: "contentTypes.xxx",
    value: "xxx",
  },
  {
    defaultLabel: "Unknown",
    labelKey: "contentTypes.unknown",
    value: "null",
  },
];

function resetContentTypeScope() {
  return "all" as const;
}

function toggleContentType(scope: ContentTypeScope, value: ContentTypeSelection): ContentTypeScope {
  if (scope === "all") {
    return [value];
  }

  if (scope.includes(value)) {
    const nextScope = scope.filter((item) => item !== value);
    return nextScope.length > 0 ? nextScope : resetContentTypeScope();
  }

  return [...scope, value];
}

function createReprocessBatchInput({
  apisDisabled,
  classifierRematch,
  contentTypeScope,
  localSearchDisabled,
  orphans,
  purge,
}: {
  apisDisabled: boolean;
  classifierRematch: boolean;
  contentTypeScope: ContentTypeScope;
  localSearchDisabled: boolean;
  orphans: boolean;
  purge: boolean;
}): QueueEnqueueReprocessTorrentsBatchInput {
  return {
    apisDisabled,
    classifierRematch,
    contentTypes:
      contentTypeScope === "all"
        ? undefined
        : contentTypeScope.map((contentType) => (contentType === "null" ? null : contentType)),
    localSearchDisabled,
    orphans: orphans ? true : undefined,
    purge,
  };
}

export function QueueReprocessBatchDialog({
  error,
  isPending,
  onClose,
  onConfirm,
  open,
}: QueueReprocessBatchDialogProps) {
  const { t } = useTranslation();
  const [acknowledged, setAcknowledged] = useState(false);
  const [purge, setPurge] = useState(true);
  const [apisDisabled, setApisDisabled] = useState(true);
  const [localSearchDisabled, setLocalSearchDisabled] = useState(true);
  const [classifierRematch, setClassifierRematch] = useState(false);
  const [orphans, setOrphans] = useState(false);
  const [contentTypeScope, setContentTypeScope] = useState<ContentTypeScope>(resetContentTypeScope);
  const dialogRef = useDialogFocus(open, onClose);

  useEffect(() => {
    if (!open) {
      return;
    }

    setAcknowledged(false);
    setPurge(true);
    setApisDisabled(true);
    setLocalSearchDisabled(true);
    setClassifierRematch(false);
    setOrphans(false);
    setContentTypeScope(resetContentTypeScope());
  }, [open]);

  if (!open) {
    return null;
  }

  const localSearchEnabled = !localSearchDisabled;
  const apiSearchEnabled = !apisDisabled;

  function confirm() {
    onConfirm(
      createReprocessBatchInput({
        apisDisabled,
        classifierRematch,
        contentTypeScope,
        localSearchDisabled,
        orphans,
        purge,
      }),
    );
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
        aria-describedby="queue-reprocess-batch-dialog-body"
        aria-labelledby="queue-reprocess-batch-dialog-title"
        aria-modal="true"
        className={styles["dialog"]}
        ref={dialogRef}
        role="dialog"
        tabIndex={-1}
      >
        <h3 id="queue-reprocess-batch-dialog-title">
          {t("queue.admin.reprocessDialogTitle", "Enqueue torrent processing batch")}
        </h3>
        <p className={styles["panelText"]} id="queue-reprocess-batch-dialog-body">
          {t(
            "queue.admin.reprocessDialogBody",
            "This will enqueue a batch reprocess job using the selected classifier and content scope.",
          )}
        </p>

        <div className={styles["dialogForm"]}>
          <label className={styles["checkboxRow"]}>
            <input
              checked={purge}
              disabled={isPending}
              onChange={(event) => setPurge(event.target.checked)}
              type="checkbox"
            />
            <span>{t("queue.admin.reprocessPurge", "Purge queue jobs")}</span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={localSearchEnabled}
              disabled={isPending}
              onChange={(event) => {
                const checked = event.target.checked;
                setLocalSearchDisabled(!checked);
                if (!checked) {
                  setApisDisabled(true);
                }
              }}
              type="checkbox"
            />
            <span>{t("queue.admin.reprocessLocalSearch", "Match content by local search")}</span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={apiSearchEnabled}
              disabled={isPending}
              onChange={(event) => setApisDisabled(!event.target.checked)}
              type="checkbox"
            />
            <span>
              {t("queue.admin.reprocessApiSearch", "Match content by external API search")}
            </span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={classifierRematch}
              disabled={isPending}
              onChange={(event) => setClassifierRematch(event.target.checked)}
              type="checkbox"
            />
            <span>{t("queue.admin.reprocessForceRematch", "Force rematch")}</span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={orphans}
              disabled={isPending}
              onChange={(event) => {
                const checked = event.target.checked;
                setOrphans(checked);
                if (checked) {
                  setContentTypeScope(resetContentTypeScope());
                }
              }}
              type="checkbox"
            />
            <span>{t("queue.admin.reprocessOrphansOnly", "Process orphaned torrents only")}</span>
          </label>

          <div className={styles["facetGroup"]}>
            <span>{t("queue.admin.reprocessContentTypes", "Content types")}</span>
            <div className={styles["chipRow"]}>
              <button
                className={styles["chip"]}
                data-active={contentTypeScope === "all"}
                disabled={isPending}
                onClick={() => setContentTypeScope(resetContentTypeScope())}
                type="button"
              >
                <span>{t("queue.admin.reprocessAllContentTypes", "All")}</span>
              </button>
              {CONTENT_TYPE_OPTIONS.map((option) => (
                <button
                  className={styles["chip"]}
                  data-active={
                    contentTypeScope !== "all" && contentTypeScope.includes(option.value)
                  }
                  disabled={isPending || orphans}
                  key={option.value}
                  onClick={() => {
                    setOrphans(false);
                    setContentTypeScope((current) => toggleContentType(current, option.value));
                  }}
                  type="button"
                >
                  <span>{t(option.labelKey, option.defaultLabel)}</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        {isPending ? (
          <p className={styles["panelText"]} role="status">
            {t("queue.admin.reprocessPending", "Enqueuing jobs")}
          </p>
        ) : null}
        {error ? (
          <p className={styles["warningText"]} role="alert">
            {t("queue.admin.reprocessDialogError", "Enqueue failed: {{error}}", {
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
              "queue.admin.reprocessAcknowledge",
              "I understand this will enqueue jobs for the selected torrent scope.",
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
            onClick={confirm}
            type="button"
          >
            {isPending
              ? t("queue.admin.reprocessEnqueuing", "Enqueuing")
              : t("queue.admin.reprocessConfirm", "Enqueue jobs")}
          </button>
        </div>
      </div>
    </div>
  );
}
