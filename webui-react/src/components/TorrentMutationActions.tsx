import type { ChangeEvent, KeyboardEvent } from "react";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { useToast } from "./toast";
import { execute } from "../graphql/client";
import {
  TorrentDeleteDocument,
  TorrentDeleteTagsDocument,
  TorrentPutTagsDocument,
  TorrentReprocessDocument,
  TorrentSetTagsDocument,
  TorrentSuggestTagsDocument,
} from "../graphql/generated/graphql";
import { useDebouncedValue } from "../utils/debounce";
import {
  DEFAULT_REPROCESS_OPTIONS,
  TAG_SUGGEST_DEBOUNCE_MS,
  addTagName,
  canConfirmDelete,
  canSubmitTagMutation,
  getErrorMessage,
  getNextReprocessOptions,
  getNextSuggestionIndex,
  getSubmittedTags,
  normalizeTagName,
  removeTagName,
  type ReprocessOptions,
  type TagMutationKind,
} from "../utils/torrentMutationActions";
import styles from "./TorrentMutationActions.module.css";

export type TorrentActionItem = {
  infoHash: string;
  magnetUri: string;
};

type TorrentMutationActionsProps = {
  className?: string;
  infoHashes: readonly string[];
  onDeleteSuccess?: () => void;
};

type TorrentBulkActionsBarProps = {
  items: readonly TorrentActionItem[];
  onClearSelection: () => void;
};

const DIALOG_FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",");

function getDialogFocusableElements(dialog: HTMLElement) {
  return Array.from(dialog.querySelectorAll<HTMLElement>(DIALOG_FOCUSABLE_SELECTOR)).filter(
    (element) => element.tabIndex >= 0,
  );
}

function useDialogFocus(open: boolean, onClose: () => void) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    if (!open) {
      return;
    }

    previousFocusRef.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;

    const dialog = dialogRef.current;
    if (!dialog) {
      return;
    }
    const activeDialog = dialog;

    (getDialogFocusableElements(activeDialog)[0] ?? activeDialog).focus();

    function handleKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const focusableElements = getDialogFocusableElements(activeDialog);
      if (focusableElements.length === 0) {
        event.preventDefault();
        activeDialog.focus();
        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements.at(-1);

      if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault();
        lastElement?.focus();
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    }

    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("keydown", handleKeyDown);

      const previousFocus = previousFocusRef.current;
      previousFocusRef.current = null;
      if (previousFocus?.isConnected) {
        previousFocus.focus();
      }
    };
  }, [open]);

  return dialogRef;
}

export function TorrentBulkActionsBar({ items, onClearSelection }: TorrentBulkActionsBarProps) {
  const notify = useToast();
  const { t } = useTranslation();
  const infoHashes = useMemo(() => items.map((item) => item.infoHash), [items]);
  const magnetLinks = useMemo(() => items.map((item) => item.magnetUri).join("\n"), [items]);
  const infoHashLines = useMemo(() => infoHashes.join("\n"), [infoHashes]);

  async function copyToClipboard(value: string, successMessage: string, failureMessage: string) {
    try {
      await navigator.clipboard.writeText(value);
      notify({ message: successMessage });
    } catch {
      notify({ message: failureMessage, tone: "error" });
    }
  }

  return (
    <section className={styles["bulkBar"]} aria-label={t("actions.bulkLabel")}>
      <div className={styles["bulkHeader"]}>
        <p className={styles["bulkTitle"]}>
          {t("actions.selectedCount", { count: infoHashes.length })}
        </p>
        <button className={styles["secondaryButton"]} onClick={onClearSelection} type="button">
          {t("actions.clearSelection")}
        </button>
      </div>

      <div className={styles["bulkActions"]}>
        <details className={styles["actionPanel"]}>
          <summary>{t("actions.copy.title")}</summary>
          <div className={styles["panelBody"]}>
            <p className={styles["panelText"]}>{t("actions.copy.body")}</p>
            <div className={styles["buttonRow"]}>
              <button
                className={styles["secondaryButton"]}
                disabled={items.length === 0}
                onClick={() =>
                  void copyToClipboard(
                    magnetLinks,
                    t("actions.copy.magnetSuccess", { count: items.length }),
                    t("actions.copy.magnetError"),
                  )
                }
                type="button"
              >
                {t("actions.copy.magnetLinks")}
              </button>
              <button
                className={styles["secondaryButton"]}
                disabled={items.length === 0}
                onClick={() =>
                  void copyToClipboard(
                    infoHashLines,
                    t("actions.copy.infoHashSuccess", { count: items.length }),
                    t("actions.copy.infoHashError"),
                  )
                }
                type="button"
              >
                {t("actions.copy.infoHashes")}
              </button>
            </div>
          </div>
        </details>

        <TorrentMutationActions
          className={styles["bulkMutationPanels"]}
          infoHashes={infoHashes}
          onDeleteSuccess={onClearSelection}
        />
      </div>
    </section>
  );
}

export function TorrentMutationActions({
  className,
  infoHashes,
  onDeleteSuccess,
}: TorrentMutationActionsProps) {
  const classNames = className ? `${styles["actionPanels"]} ${className}` : styles["actionPanels"];

  return (
    <div className={classNames}>
      <TagActions infoHashes={infoHashes} />
      <ReprocessActions infoHashes={infoHashes} />
      <DeleteActions infoHashes={infoHashes} onDeleteSuccess={onDeleteSuccess} />
    </div>
  );
}

function TagActions({ infoHashes }: { infoHashes: readonly string[] }) {
  const [tagInput, setTagInput] = useState("");
  const [tagNames, setTagNames] = useState<string[]>([]);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(-1);
  const debouncedTagInput = useDebouncedValue(tagInput, TAG_SUGGEST_DEBOUNCE_MS);
  const prefix = normalizeTagName(debouncedTagInput);
  const listboxId = useId();
  const notify = useToast();
  const queryClient = useQueryClient();
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;

  const {
    data: suggestionData,
    error: suggestionError,
    isError: isSuggestionError,
  } = useQuery({
    enabled: prefix.length > 0,
    queryFn: ({ signal }) =>
      execute(
        TorrentSuggestTagsDocument,
        {
          input: {
            exclusions: tagNames,
            prefix,
          },
        },
        signal,
      ),
    queryKey: ["torrentSuggestTags", prefix, tagNames],
  });

  const suggestions = useMemo(() => {
    if (!prefix) {
      return [];
    }

    return (
      suggestionData?.torrent.suggestTags.suggestions.filter(
        (suggestion) => !tagNames.includes(suggestion.name),
      ) ?? []
    );
  }, [prefix, suggestionData, tagNames]);

  const tagMutation = useMutation({
    mutationFn: async ({
      infoHashes: mutationInfoHashes,
      kind,
      tagNames: mutationTagNames,
    }: {
      infoHashes: string[];
      kind: TagMutationKind;
      tagNames: string[];
    }) => {
      if (kind === "put") {
        await execute(TorrentPutTagsDocument, {
          infoHashes: mutationInfoHashes,
          tagNames: mutationTagNames,
        });
        return;
      }

      if (kind === "set") {
        await execute(TorrentSetTagsDocument, {
          infoHashes: mutationInfoHashes,
          tagNames: mutationTagNames,
        });
        return;
      }

      await execute(TorrentDeleteTagsDocument, {
        infoHashes: mutationInfoHashes,
        tagNames: mutationTagNames,
      });
    },
    onError: (error) => {
      notify({
        message: t("actions.tags.error", { error: getErrorMessage(error) }),
        tone: "error",
      });
    },
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["torrentContentSearch"] });
      void queryClient.invalidateQueries({ queryKey: ["torrentDetail"] });
      notify({
        message: t(`actions.tags.${variables.kind}Success`, {
          count: variables.infoHashes.length,
          tagCount: variables.tagNames.length,
        }),
      });
    },
  });

  useEffect(() => {
    if (!isSuggestionError) {
      return;
    }

    notify({
      message: t("actions.tags.suggestionError", {
        error: getErrorMessage(suggestionError),
      }),
      tone: "error",
    });
  }, [isSuggestionError, notify, suggestionError, t]);

  useEffect(() => {
    setActiveSuggestionIndex((currentIndex) =>
      currentIndex >= suggestions.length ? -1 : currentIndex,
    );
  }, [suggestions.length]);

  function addTag(tagName: string) {
    const nextTags = addTagName(tagNames, tagName);
    setTagNames(nextTags);
    setTagInput("");
    setActiveSuggestionIndex(-1);
  }

  function submitTagMutation(kind: TagMutationKind) {
    if (!canSubmitTagMutation(kind, infoHashes.length, tagNames, tagInput, tagMutation.isPending)) {
      return;
    }

    const nextTagNames = getSubmittedTags(tagNames, tagInput);
    setTagNames(nextTagNames);
    setTagInput("");
    setActiveSuggestionIndex(-1);
    tagMutation.mutate({
      infoHashes: [...infoHashes],
      kind,
      tagNames: nextTagNames,
    });
  }

  function handleTagKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveSuggestionIndex((currentIndex) =>
        getNextSuggestionIndex(currentIndex, suggestions.length, "down"),
      );
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveSuggestionIndex((currentIndex) =>
        getNextSuggestionIndex(currentIndex, suggestions.length, "up"),
      );
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const activeSuggestion = suggestions[activeSuggestionIndex];
      addTag(activeSuggestion?.name ?? tagInput);
      return;
    }

    if (event.key === ",") {
      event.preventDefault();
      addTag(tagInput);
    }
  }

  const inputDisabled = tagMutation.isPending;
  const suggestionsOpen = suggestions.length > 0;
  const activeSuggestionId =
    activeSuggestionIndex >= 0 ? `${listboxId}-${activeSuggestionIndex}` : undefined;

  return (
    <details className={styles["actionPanel"]}>
      <summary>{t("actions.tags.title")}</summary>
      <div className={styles["panelBody"]}>
        <label className={styles["tagField"]}>
          <span>{t("actions.tags.inputLabel")}</span>
          <div className={styles["tagInputWrap"]}>
            {tagNames.length > 0 ? (
              <ul className={styles["chipList"]}>
                {tagNames.map((tagName) => (
                  <li className={styles["chip"]} key={tagName}>
                    <span>{tagName}</span>
                    <button
                      aria-label={t("actions.tags.removeChip", { tagName })}
                      disabled={inputDisabled}
                      onClick={() => setTagNames(removeTagName(tagNames, tagName))}
                      type="button"
                    >
                      x
                    </button>
                  </li>
                ))}
              </ul>
            ) : null}
            <input
              aria-activedescendant={activeSuggestionId}
              aria-autocomplete="list"
              aria-controls={suggestionsOpen ? listboxId : undefined}
              aria-expanded={suggestionsOpen}
              aria-haspopup="listbox"
              autoComplete="off"
              className={styles["tagInput"]}
              disabled={inputDisabled}
              onChange={(event: ChangeEvent<HTMLInputElement>) => setTagInput(event.target.value)}
              onKeyDown={handleTagKeyDown}
              placeholder={t("actions.tags.placeholder")}
              role="combobox"
              type="text"
              value={tagInput}
            />
          </div>
          {suggestionsOpen ? (
            <div
              aria-label={t("actions.tags.suggestionsLabel")}
              className={styles["suggestions"]}
              id={listboxId}
              role="listbox"
            >
              {suggestions.map((suggestion, index) => (
                <button
                  aria-selected={index === activeSuggestionIndex}
                  className={styles["suggestion"]}
                  id={`${listboxId}-${index}`}
                  key={suggestion.name}
                  onClick={() => addTag(suggestion.name)}
                  onMouseDown={(event) => event.preventDefault()}
                  role="option"
                  type="button"
                >
                  <span>{suggestion.name}</span>
                  <small>{suggestion.count.toLocaleString(locale)}</small>
                </button>
              ))}
            </div>
          ) : null}
        </label>

        <div className={styles["buttonRow"]}>
          <button
            className={styles["secondaryButton"]}
            disabled={
              !canSubmitTagMutation(
                "put",
                infoHashes.length,
                tagNames,
                tagInput,
                tagMutation.isPending,
              )
            }
            onClick={() => submitTagMutation("put")}
            type="button"
          >
            {t("actions.tags.put")}
          </button>
          <button
            className={styles["secondaryButton"]}
            disabled={
              !canSubmitTagMutation(
                "set",
                infoHashes.length,
                tagNames,
                tagInput,
                tagMutation.isPending,
              )
            }
            onClick={() => submitTagMutation("set")}
            type="button"
          >
            {t("actions.tags.set")}
          </button>
          <button
            className={styles["secondaryButton"]}
            disabled={
              !canSubmitTagMutation(
                "delete",
                infoHashes.length,
                tagNames,
                tagInput,
                tagMutation.isPending,
              )
            }
            onClick={() => submitTagMutation("delete")}
            type="button"
          >
            {t("actions.tags.delete")}
          </button>
        </div>
      </div>
    </details>
  );
}

function ReprocessActions({ infoHashes }: { infoHashes: readonly string[] }) {
  const [options, setOptions] = useState<ReprocessOptions>(DEFAULT_REPROCESS_OPTIONS);
  const notify = useToast();
  const queryClient = useQueryClient();
  const { t } = useTranslation();

  const reprocessMutation = useMutation({
    mutationFn: async ({
      infoHashes: mutationInfoHashes,
      options: mutationOptions,
    }: {
      infoHashes: string[];
      options: ReprocessOptions;
    }) => {
      await execute(TorrentReprocessDocument, {
        input: {
          apisDisabled: mutationOptions.apisDisabled,
          classifierRematch: mutationOptions.classifierRematch,
          infoHashes: mutationInfoHashes,
          localSearchDisabled: mutationOptions.localSearchDisabled,
        },
      });
    },
    onError: (error) => {
      notify({
        message: t("actions.reprocess.error", { error: getErrorMessage(error) }),
        tone: "error",
      });
    },
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["torrentContentSearch"] });
      void queryClient.invalidateQueries({ queryKey: ["torrentDetail"] });
      notify({
        message: t("actions.reprocess.success", { count: variables.infoHashes.length }),
      });
    },
  });

  function updateOption(field: "apis" | "classifier" | "local", checked: boolean) {
    setOptions((current) => getNextReprocessOptions(current, field, checked));
  }

  function submitReprocess() {
    if (infoHashes.length === 0 || reprocessMutation.isPending) {
      return;
    }

    reprocessMutation.mutate({
      infoHashes: [...infoHashes],
      options,
    });
  }

  return (
    <details className={styles["actionPanel"]}>
      <summary>{t("actions.reprocess.title")}</summary>
      <div className={styles["panelBody"]}>
        <fieldset className={styles["checkboxList"]} disabled={reprocessMutation.isPending}>
          <legend className={styles["checkboxLegend"]}>{t("actions.reprocess.options")}</legend>
          <label className={styles["checkboxRow"]}>
            <input
              checked={!options.localSearchDisabled}
              onChange={(event) => updateOption("local", event.target.checked)}
              type="checkbox"
            />
            <span>{t("actions.reprocess.localSearch")}</span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={!options.apisDisabled}
              onChange={(event) => updateOption("apis", event.target.checked)}
              type="checkbox"
            />
            <span>{t("actions.reprocess.externalApiSearch")}</span>
          </label>
          <label className={styles["checkboxRow"]}>
            <input
              checked={options.classifierRematch}
              onChange={(event) => updateOption("classifier", event.target.checked)}
              type="checkbox"
            />
            <span>{t("actions.reprocess.forceRematch")}</span>
          </label>
        </fieldset>
        <div className={styles["buttonRow"]}>
          <button
            className={styles["secondaryButton"]}
            disabled={infoHashes.length === 0 || reprocessMutation.isPending}
            onClick={submitReprocess}
            type="button"
          >
            {t("actions.reprocess.submit")}
          </button>
        </div>
      </div>
    </details>
  );
}

function DeleteActions({
  infoHashes,
  onDeleteSuccess,
}: {
  infoHashes: readonly string[];
  onDeleteSuccess?: () => void;
}) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [acknowledged, setAcknowledged] = useState(false);
  const notify = useToast();
  const queryClient = useQueryClient();
  const { t } = useTranslation();

  const deleteMutation = useMutation({
    mutationFn: async ({ infoHashes: mutationInfoHashes }: { infoHashes: string[] }) => {
      await execute(TorrentDeleteDocument, {
        infoHashes: mutationInfoHashes,
      });
    },
    onError: (error) => {
      notify({
        message: t("actions.delete.error", { error: getErrorMessage(error) }),
        tone: "error",
      });
    },
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["torrentContentSearch"] });
      void queryClient.invalidateQueries({ queryKey: ["torrentDetail"] });
      setDialogOpen(false);
      setAcknowledged(false);
      notify({
        message: t("actions.delete.success", { count: variables.infoHashes.length }),
      });
      onDeleteSuccess?.();
    },
  });

  function closeDialog() {
    if (deleteMutation.isPending) {
      return;
    }

    setDialogOpen(false);
    setAcknowledged(false);
  }

  function openDialog() {
    if (infoHashes.length === 0 || deleteMutation.isPending) {
      return;
    }

    setDialogOpen(true);
  }

  function confirmDelete() {
    if (!canConfirmDelete(infoHashes.length, acknowledged, deleteMutation.isPending)) {
      return;
    }

    deleteMutation.mutate({ infoHashes: [...infoHashes] });
  }

  const dialogRef = useDialogFocus(dialogOpen, closeDialog);

  return (
    <details className={styles["actionPanel"]}>
      <summary>{t("actions.delete.title")}</summary>
      <div className={styles["panelBody"]}>
        <p className={styles["warningText"]}>{t("actions.delete.warning")}</p>
        <div className={styles["buttonRow"]}>
          <button
            className={styles["dangerButton"]}
            disabled={infoHashes.length === 0 || deleteMutation.isPending}
            onClick={openDialog}
            type="button"
          >
            {t("actions.delete.open")}
          </button>
        </div>
      </div>

      {dialogOpen ? (
        <div
          className={styles["dialogBackdrop"]}
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeDialog();
            }
          }}
          role="presentation"
        >
          <div
            aria-labelledby="torrent-delete-dialog-title"
            aria-modal="true"
            className={styles["dialog"]}
            ref={dialogRef}
            role="dialog"
            tabIndex={-1}
          >
            <h3 id="torrent-delete-dialog-title">
              {t("actions.delete.dialogTitle", { count: infoHashes.length })}
            </h3>
            <p className={styles["panelText"]}>
              {t("actions.delete.dialogBody", { count: infoHashes.length })}
            </p>
            <p className={styles["warningText"]}>{t("actions.delete.warning")}</p>
            <label className={styles["checkboxRow"]}>
              <input
                checked={acknowledged}
                disabled={deleteMutation.isPending}
                onChange={(event) => setAcknowledged(event.target.checked)}
                type="checkbox"
              />
              <span>{t("actions.delete.acknowledge")}</span>
            </label>
            <div className={styles["dialogActions"]}>
              <button
                className={styles["secondaryButton"]}
                disabled={deleteMutation.isPending}
                onClick={closeDialog}
                type="button"
              >
                {t("actions.delete.cancel")}
              </button>
              <button
                className={styles["dangerButton"]}
                disabled={
                  !canConfirmDelete(infoHashes.length, acknowledged, deleteMutation.isPending)
                }
                onClick={confirmDelete}
                type="button"
              >
                {t("actions.delete.confirm")}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </details>
  );
}
