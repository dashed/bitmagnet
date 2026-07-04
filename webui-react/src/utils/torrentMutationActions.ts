export const TAG_SUGGEST_DEBOUNCE_MS = 300;

export type TagMutationKind = "delete" | "put" | "set";

export type ReprocessOptions = {
  apisDisabled: boolean;
  classifierRematch: boolean;
  localSearchDisabled: boolean;
};

export const DEFAULT_REPROCESS_OPTIONS: ReprocessOptions = {
  apisDisabled: true,
  classifierRematch: false,
  localSearchDisabled: true,
};

export function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

export function normalizeTagName(tagName: string) {
  return tagName.trim();
}

export function addTagName(tags: readonly string[], tagName: string) {
  const normalized = normalizeTagName(tagName);

  if (!normalized || tags.includes(normalized)) {
    return [...tags];
  }

  return [...tags, normalized];
}

export function removeTagName(tags: readonly string[], tagName: string) {
  return tags.filter((tag) => tag !== tagName);
}

export function renameTagName(
  tags: readonly string[],
  previousTagName: string,
  nextTagName: string,
) {
  const normalized = normalizeTagName(nextTagName);

  if (!normalized) {
    return removeTagName(tags, previousTagName);
  }

  const renamed = tags.map((tag) => (tag === previousTagName ? normalized : tag));

  return Array.from(new Set(renamed));
}

export function getSubmittedTags(tags: readonly string[], draftTagName: string) {
  return addTagName(tags, draftTagName);
}

export function canSubmitTagMutation(
  kind: TagMutationKind,
  infoHashCount: number,
  tagNames: readonly string[],
  draftTagName: string,
  isPending: boolean,
) {
  if (infoHashCount === 0 || isPending) {
    return false;
  }

  if (kind === "set") {
    return true;
  }

  return getSubmittedTags(tagNames, draftTagName).length > 0;
}

export function getNextSuggestionIndex(
  currentIndex: number,
  suggestionCount: number,
  direction: "down" | "up",
) {
  if (suggestionCount <= 0) {
    return -1;
  }

  if (direction === "down") {
    return currentIndex < suggestionCount - 1 ? currentIndex + 1 : 0;
  }

  return currentIndex > 0 ? currentIndex - 1 : suggestionCount - 1;
}

export function canConfirmDelete(infoHashCount: number, acknowledged: boolean, isPending: boolean) {
  return infoHashCount > 0 && acknowledged && !isPending;
}

export function getNextReprocessOptions(
  current: ReprocessOptions,
  field: "apis" | "classifier" | "local",
  checked: boolean,
): ReprocessOptions {
  switch (field) {
    case "apis":
      return {
        ...current,
        apisDisabled: !checked,
        localSearchDisabled: checked ? false : current.localSearchDisabled,
      };
    case "classifier":
      return {
        ...current,
        classifierRematch: checked,
      };
    case "local":
      return {
        ...current,
        apisDisabled: checked ? current.apisDisabled : true,
        localSearchDisabled: !checked,
      };
  }
}
