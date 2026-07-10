import type { ChangeEvent, FormEvent, InputHTMLAttributes, ReactNode } from "react";
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../components/ListSkeleton";
import { QueryError } from "../components/QueryError";
import type { TorrentActionItem } from "../components/TorrentMutationActions";
import { useToast } from "../components/toast";
import { isSearchModesEnabled } from "../flags";
import { execute } from "../graphql/client";
import { TorrentContentSearchDocument } from "../graphql/generated/graphql";
import type {
  ContentTypeAgg,
  GenreAgg,
  LanguageAgg,
  TorrentContentSearchQuery,
  TorrentFileTypeAgg,
  TorrentSourceAgg,
  TorrentTagAgg,
  VideoResolutionAgg,
  VideoSourceAgg,
} from "../graphql/generated/graphql";
import {
  addSavedSearch,
  deleteSavedSearch,
  renameSavedSearch,
  type SavedSearch,
  useSavedSearches,
} from "../searches/savedSearches";
import { useDialogFocus } from "../utils/dialogFocus";
import { formatFileSize } from "../utils/filesize";
import { formatIntEstimate } from "../utils/intEstimate";
import { formatRelativeTime } from "../utils/relativeTime";
import {
  DEFAULT_SIZE_UNIT,
  ORDER_OPTIONS,
  PAGE_SIZE_OPTIONS,
  PUBLISHED_PRESETS,
  SEARCH_MODES,
  SIZE_UNITS,
  TORRENT_SEARCH_FACET_KEYS,
  formatPublishedRangeValue,
  getDefaultDescending,
  getPublishedRangeInputValues,
  getTorrentSearchFacets,
  getTorrentSearchOrderBy,
  isPublishedPreset,
  isTorrentSearchFacetRelevant,
  parseTorrentSearchParams,
  sanitizeFacetSelections,
  stringifyTorrentSearchParams,
  type TorrentSearchUrlParams,
  updateQuery,
  updateSearchMode,
} from "./searchParams";
import type {
  ContentTypeSelection,
  SearchMode,
  SizeUnit,
  TorrentSearchFacetKey,
  TorrentSearchFacetSelections,
  TorrentSearchState,
} from "./searchParams";
import {
  clearSelectionOnSearchParamsChange,
  getPageSelectionState,
  toggleInfoHashSelection,
  togglePageSelection,
} from "./searchSelection";
import styles from "./SearchPage.module.css";

const LazyTorrentBulkActionsBar = lazy(async () => {
  const module = await import("../components/TorrentMutationActions");

  return { default: module.TorrentBulkActionsBar };
});
const LazyFileSearchView = lazy(() => import("./searchModes/FileSearchView"));
const LazyPathBrowseView = lazy(() => import("./searchModes/PathBrowseView"));
const PUBLISHED_CUSTOM_RANGE_VALUE = "__custom_range";

type SearchResult = TorrentContentSearchQuery["torrentContent"]["search"];
type SearchItem = SearchResult["items"][number];
const EMPTY_SEARCH_ITEMS: SearchItem[] = [];
type SearchAggregations = SearchResult["aggregations"];
type SizeDraft = {
  max: string;
  maxUnit: SizeUnit;
  min: string;
  minUnit: SizeUnit;
};
type PublishedRangeDraft = {
  end: string;
  start: string;
};
type SearchSelectionState = {
  infoHashes: Set<string>;
  searchParamsKey: string;
};
type ContentTypeOption = {
  count: number;
  isEstimate: boolean;
  label: string;
  value: ContentTypeSelection;
};
type DynamicFacetAgg =
  | GenreAgg
  | LanguageAgg
  | TorrentFileTypeAgg
  | TorrentSourceAgg
  | TorrentTagAgg
  | VideoResolutionAgg
  | VideoSourceAgg;
type DynamicFacetOption = {
  count: number;
  isEstimate: boolean;
  label: string;
  value: string;
};
type SavedSearchControlsProps = {
  params: TorrentSearchUrlParams;
  suggestedName: string;
};

function IndeterminateCheckbox({
  indeterminate,
  ...props
}: InputHTMLAttributes<HTMLInputElement> & { indeterminate: boolean }) {
  const checkboxRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (checkboxRef.current) {
      checkboxRef.current.indeterminate = indeterminate;
    }
  }, [indeterminate]);

  return <input {...props} ref={checkboxRef} type="checkbox" />;
}

function getPeerCount(value: number | null | undefined) {
  return value ?? 0;
}

function getResultTitle(item: SearchItem) {
  return item.title.trim() || item.torrent.name;
}

function getPeerLabel(item: SearchItem, locale: string) {
  return `${getPeerCount(item.seeders).toLocaleString(locale)} / ${getPeerCount(
    item.leechers,
  ).toLocaleString(locale)}`;
}

function getSizeDraft(search: TorrentSearchState): SizeDraft {
  return {
    max: search.maxSize?.toString() ?? "",
    maxUnit: search.maxSizeUnit,
    min: search.minSize?.toString() ?? "",
    minUnit: search.minSizeUnit,
  };
}

function parseSizeInput(value: string) {
  const trimmed = value.trim();

  if (!trimmed) {
    return undefined;
  }

  const parsed = Number.parseInt(trimmed, 10);

  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function getPublishedRangeDraft(value: string | undefined): PublishedRangeDraft {
  return getPublishedRangeInputValues(value) ?? { end: "", start: "" };
}

function isPublishedRangeDraftComplete(draft: PublishedRangeDraft) {
  return Boolean(draft.start && draft.end);
}

function isPublishedRangeDraftInvalid(draft: PublishedRangeDraft) {
  return isPublishedRangeDraftComplete(draft) && !formatPublishedRangeValue(draft.start, draft.end);
}

function getDhtSeenTooltip(
  item: SearchItem,
  labels: { count: string; first: string; last: string },
) {
  if (!item.dhtLastSeenAt) {
    return "";
  }

  const fmt = (iso: string) => new Date(iso).toLocaleString();

  return [
    item.dhtFirstSeenAt ? `${labels.first}: ${fmt(item.dhtFirstSeenAt)}` : undefined,
    `${labels.last}: ${fmt(item.dhtLastSeenAt)}`,
    `${labels.count}: ${item.dhtSeenCount.toLocaleString()}`,
  ]
    .filter(Boolean)
    .join("\n");
}

function getContentTypeLabel(value: ContentTypeSelection | null, t: (key: string) => string) {
  return value && value !== "null" ? t(`contentTypes.${value}`) : t("contentTypes.unknown");
}

function getContentTypeAggKey(agg: ContentTypeAgg): ContentTypeSelection {
  return agg.value ?? "null";
}

function getSelectedFacetKeys(selections: TorrentSearchFacetSelections) {
  return TORRENT_SEARCH_FACET_KEYS.filter((key) => (selections[key]?.length ?? 0) > 0);
}

function areFacetKeySetsEqual(
  left: ReadonlySet<TorrentSearchFacetKey>,
  right: ReadonlySet<TorrentSearchFacetKey>,
) {
  if (left.size !== right.size) {
    return false;
  }

  for (const key of left) {
    if (!right.has(key)) {
      return false;
    }
  }

  return true;
}

function getDynamicFacetAggregations(
  aggregations: SearchAggregations | undefined,
  key: TorrentSearchFacetKey,
): DynamicFacetAgg[] {
  if (!aggregations) {
    return [];
  }

  switch (key) {
    case "file_type":
      return aggregations.torrentFileType ?? [];
    case "genre":
      return aggregations.genre ?? [];
    case "language":
      return aggregations.language ?? [];
    case "torrent_source":
      return aggregations.torrentSource ?? [];
    case "torrent_tag":
      return aggregations.torrentTag ?? [];
    case "video_resolution":
      return aggregations.videoResolution ?? [];
    case "video_source":
      return aggregations.videoSource ?? [];
  }
}

function getFacetValue(agg: DynamicFacetAgg) {
  return agg.value === null || agg.value === undefined ? "null" : String(agg.value);
}

function getFacetValueLabel(key: TorrentSearchFacetKey, value: string, t: (key: string) => string) {
  if (value === "null" && (key === "video_resolution" || key === "video_source")) {
    return t("facets.unknown");
  }

  if (key === "file_type") {
    return t(`fileTypes.${value}`);
  }

  if (key === "video_resolution" && value.startsWith("V")) {
    return value.slice(1);
  }

  return value;
}

function getFacetAggLabel(
  key: TorrentSearchFacetKey,
  agg: DynamicFacetAgg,
  t: (key: string) => string,
) {
  if (getFacetValue(agg) === "null" && (key === "video_resolution" || key === "video_source")) {
    return t("facets.unknown");
  }

  if (key === "file_type") {
    return t(`fileTypes.${agg.value}`);
  }

  if (key === "torrent_tag") {
    return String(agg.value);
  }

  return agg.label;
}

function getDynamicFacetOptions(
  aggregations: SearchAggregations | undefined,
  key: TorrentSearchFacetKey,
  selectedValues: readonly string[],
  t: (key: string) => string,
) {
  const options = new Map<string, DynamicFacetOption>();

  for (const agg of getDynamicFacetAggregations(aggregations, key)) {
    const value = getFacetValue(agg);
    options.set(value, {
      count: agg.count,
      isEstimate: agg.isEstimate,
      label: getFacetAggLabel(key, agg, t),
      value,
    });
  }

  for (const value of selectedValues) {
    if (!options.has(value)) {
      options.set(value, {
        count: 0,
        isEstimate: false,
        label: getFacetValueLabel(key, value, t),
        value,
      });
    }
  }

  return Array.from(options.values());
}

function updateFacetSelection(
  selections: TorrentSearchFacetSelections,
  key: TorrentSearchFacetKey,
  value: string,
  checked: boolean,
) {
  const next: TorrentSearchFacetSelections = { ...selections };
  const currentValues = selections[key] ?? [];
  const nextValues = checked
    ? Array.from(new Set([...currentValues, value])).sort()
    : currentValues.filter((currentValue) => currentValue !== value);

  if (nextValues.length) {
    next[key] = nextValues;
  } else {
    delete next[key];
  }

  return next;
}

function getContentTypeOptions(
  aggregations: ContentTypeAgg[],
  selected: ContentTypeSelection | undefined,
  t: (key: string) => string,
): ContentTypeOption[] {
  const options = aggregations
    .map((agg) => ({
      count: agg.count,
      isEstimate: agg.isEstimate,
      label: getContentTypeLabel(agg.value ?? "null", t),
      value: getContentTypeAggKey(agg),
    }))
    .sort((left, right) => left.label.localeCompare(right.label));

  if (selected && !options.some((option) => option.value === selected)) {
    options.push({
      count: 0,
      isEstimate: false,
      label: getContentTypeLabel(selected, t),
      value: selected,
    });
  }

  return options;
}

function SavedSearchControls({ params, suggestedName }: SavedSearchControlsProps) {
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameName, setRenameName] = useState("");
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [saveName, setSaveName] = useState("");
  const menuRef = useRef<HTMLDetailsElement>(null);
  const navigate = useNavigate({ from: "/" });
  const notify = useToast();
  const savedSearches = useSavedSearches();
  const { t } = useTranslation();

  function closeSaveDialog() {
    setSaveDialogOpen(false);
  }

  function openSaveDialog() {
    setSaveName(suggestedName);
    setSaveDialogOpen(true);
  }

  function handleSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const savedSearch = addSavedSearch(saveName, params);

    if (!savedSearch) {
      return;
    }

    closeSaveDialog();
    notify({ message: t("savedSearches.saved") });
  }

  function handleApply(item: SavedSearch) {
    menuRef.current?.removeAttribute("open");
    void navigate({
      search: item.params,
      to: "/",
    });
  }

  function beginRename(item: SavedSearch) {
    setRenameName(item.name);
    setRenamingId(item.id);
  }

  function cancelRename() {
    setRenameName("");
    setRenamingId(null);
  }

  function handleRename(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!renamingId || !renameSavedSearch(renamingId, renameName)) {
      return;
    }

    cancelRename();
  }

  function handleDelete(id: string) {
    deleteSavedSearch(id);

    if (renamingId === id) {
      cancelRename();
    }
  }

  const saveDialogRef = useDialogFocus(saveDialogOpen, closeSaveDialog);

  return (
    <div className={styles["savedSearchToolbar"]}>
      <button className={styles["submitSmall"]} onClick={openSaveDialog} type="button">
        {t("savedSearches.save")}
      </button>
      <details className={styles["savedSearchMenu"]} ref={menuRef}>
        <summary aria-label={t("savedSearches.title")}>
          <span>{t("savedSearches.title")}</span>
          {savedSearches.length > 0 ? (
            <span className={styles["savedSearchCount"]}>{savedSearches.length}</span>
          ) : null}
        </summary>
        <div className={styles["savedSearchMenuBody"]}>
          {savedSearches.length > 0 ? (
            <ul className={styles["savedSearchList"]}>
              {savedSearches.map((item) => (
                <li className={styles["savedSearchRow"]} key={item.id}>
                  {renamingId === item.id ? (
                    <form className={styles["savedSearchRenameForm"]} onSubmit={handleRename}>
                      <input
                        aria-label={t("savedSearches.renameNamed", { name: item.name })}
                        autoComplete="off"
                        autoFocus
                        className={styles["savedSearchRenameInput"]}
                        onChange={(event) => setRenameName(event.target.value)}
                        type="text"
                        value={renameName}
                      />
                      <div className={styles["savedSearchRenameActions"]}>
                        <button
                          className={styles["savedSearchAction"]}
                          disabled={!renameName.trim()}
                          type="submit"
                        >
                          {t("savedSearches.confirm")}
                        </button>
                        <button
                          className={styles["savedSearchAction"]}
                          onClick={cancelRename}
                          type="button"
                        >
                          {t("savedSearches.cancel")}
                        </button>
                      </div>
                    </form>
                  ) : (
                    <>
                      <button
                        className={styles["savedSearchApply"]}
                        onClick={() => handleApply(item)}
                        title={t("savedSearches.apply")}
                        type="button"
                      >
                        {item.name}
                      </button>
                      <button
                        aria-label={t("savedSearches.renameNamed", { name: item.name })}
                        className={styles["savedSearchAction"]}
                        onClick={() => beginRename(item)}
                        type="button"
                      >
                        {t("savedSearches.rename")}
                      </button>
                      <button
                        aria-label={t("savedSearches.deleteNamed", { name: item.name })}
                        className={`${styles["savedSearchAction"]} ${styles["savedSearchDelete"]}`}
                        onClick={() => handleDelete(item.id)}
                        type="button"
                      >
                        {t("savedSearches.delete")}
                      </button>
                    </>
                  )}
                </li>
              ))}
            </ul>
          ) : (
            <p className={styles["savedSearchEmpty"]}>{t("savedSearches.empty")}</p>
          )}
        </div>
      </details>

      {saveDialogOpen ? (
        <div
          className={styles["saveDialogBackdrop"]}
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeSaveDialog();
            }
          }}
          role="presentation"
        >
          <div
            aria-labelledby="save-search-dialog-title"
            aria-modal="true"
            className={styles["saveDialog"]}
            ref={saveDialogRef}
            role="dialog"
            tabIndex={-1}
          >
            <h3 id="save-search-dialog-title">{t("savedSearches.save")}</h3>
            <form className={styles["saveDialogForm"]} onSubmit={handleSave}>
              <label className={styles["saveDialogField"]} htmlFor="save-search-name">
                <span>{t("savedSearches.nameLabel")}</span>
                <input
                  autoComplete="off"
                  id="save-search-name"
                  onChange={(event) => setSaveName(event.target.value)}
                  placeholder={t("savedSearches.namePlaceholder")}
                  type="text"
                  value={saveName}
                />
              </label>
              <div className={styles["saveDialogActions"]}>
                <button
                  className={styles["secondaryButton"]}
                  onClick={closeSaveDialog}
                  type="button"
                >
                  {t("savedSearches.cancel")}
                </button>
                <button
                  className={styles["submitSmall"]}
                  disabled={!saveName.trim()}
                  type="submit"
                >
                  {t("savedSearches.confirm")}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function SearchPage() {
  const routeSearch = useSearch({ from: "/" });
  const search = useMemo(() => parseTorrentSearchParams(routeSearch), [routeSearch]);
  const searchModesEnabled = isSearchModesEnabled();
  const effectiveMode: SearchMode = searchModesEnabled ? search.mode : "torrents";
  const searchParams = useMemo(() => stringifyTorrentSearchParams(search), [search]);
  const searchParamsKey = useMemo(() => JSON.stringify(searchParams), [searchParams]);
  const sanitizedFacetSelections = useMemo(
    () => sanitizeFacetSelections(search.facets, search.contentType),
    [search.contentType, search.facets],
  );
  const selectedFacetKeys = useMemo(
    () => getSelectedFacetKeys(sanitizedFacetSelections),
    [sanitizedFacetSelections],
  );
  const [expandedFacets, setExpandedFacets] = useState<Set<TorrentSearchFacetKey>>(
    () => new Set(selectedFacetKeys),
  );
  const activeFacetKeys = useMemo(
    () =>
      TORRENT_SEARCH_FACET_KEYS.filter(
        (key) =>
          isTorrentSearchFacetRelevant(key, search.contentType) &&
          (expandedFacets.has(key) || selectedFacetKeys.includes(key)),
      ),
    [expandedFacets, search.contentType, selectedFacetKeys],
  );
  const relevantFacetKeys = useMemo(
    () =>
      TORRENT_SEARCH_FACET_KEYS.filter((key) =>
        isTorrentSearchFacetRelevant(key, search.contentType),
      ),
    [search.contentType],
  );
  const searchKey = useMemo(
    () =>
      JSON.stringify({
        activeFacetKeys,
        searchParams,
      }),
    [activeFacetKeys, searchParams],
  );
  const [draftQuery, setDraftQuery] = useState(search.query);
  const [sizeDraft, setSizeDraft] = useState<SizeDraft>(() => getSizeDraft(search));
  const [publishedRangeDraft, setPublishedRangeDraft] = useState<PublishedRangeDraft>(() =>
    getPublishedRangeDraft(search.publishedAt),
  );
  const [publishedRangeOpen, setPublishedRangeOpen] = useState(() =>
    Boolean(search.publishedAt && !isPublishedPreset(search.publishedAt)),
  );
  const [refresh, setRefresh] = useState<{ nonce: number; uncachedSearchKey: string | null }>({
    nonce: 0,
    uncachedSearchKey: null,
  });
  const [selection, setSelection] = useState<SearchSelectionState>(() => ({
    infoHashes: new Set(),
    searchParamsKey,
  }));
  const filtersRef = useRef<HTMLDetailsElement>(null);
  const navigate = useNavigate({ from: "/" });
  const notify = useToast();
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  let selectedInfoHashes = selection.infoHashes;

  if (selection.searchParamsKey !== searchParamsKey) {
    selectedInfoHashes = clearSelectionOnSearchParamsChange(
      selection.infoHashes,
      selection.searchParamsKey,
      searchParamsKey,
    );
    setSelection({
      infoHashes: selectedInfoHashes,
      searchParamsKey,
    });
  }

  useEffect(() => {
    setDraftQuery(search.query);
  }, [search.query]);

  useEffect(() => {
    setSizeDraft(getSizeDraft(search));
  }, [search]);

  useEffect(() => {
    if (search.publishedAt && !isPublishedPreset(search.publishedAt)) {
      setPublishedRangeOpen(true);
      setPublishedRangeDraft(getPublishedRangeDraft(search.publishedAt));

      return;
    }

    if (search.publishedAt) {
      setPublishedRangeOpen(false);
      setPublishedRangeDraft({ end: "", start: "" });

      return;
    }

    setPublishedRangeDraft({ end: "", start: "" });
  }, [search.publishedAt]);

  useEffect(() => {
    setExpandedFacets((currentFacets) => {
      const nextFacets = new Set<TorrentSearchFacetKey>();

      for (const key of currentFacets) {
        if (isTorrentSearchFacetRelevant(key, search.contentType)) {
          nextFacets.add(key);
        }
      }

      for (const key of selectedFacetKeys) {
        nextFacets.add(key);
      }

      return areFacetKeySetsEqual(currentFacets, nextFacets) ? currentFacets : nextFacets;
    });
  }, [search.contentType, selectedFacetKeys]);

  function navigateSearch(nextSearch: TorrentSearchState, replace = true) {
    void navigate({
      replace,
      resetScroll: false,
      search: stringifyTorrentSearchParams(nextSearch),
      to: "/",
    });
  }

  function handleSearchModeChange(mode: SearchMode) {
    navigateSearch(updateSearchMode(search, mode));
  }

  function handleQueryChange(event: ChangeEvent<HTMLInputElement>) {
    const nextValue = event.target.value;
    setDraftQuery(nextValue);

    if (nextValue.trim() === "" && search.query) {
      navigateSearch(updateQuery(search, ""));
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    navigateSearch(updateQuery(search, draftQuery));
  }

  function handleRefresh() {
    setRefresh((currentRefresh) => ({
      nonce: currentRefresh.nonce + 1,
      uncachedSearchKey: searchKey,
    }));
  }

  function handlePageChange(page: number) {
    navigateSearch({ ...search, page }, false);
  }

  function handlePageSizeChange(event: ChangeEvent<HTMLSelectElement>) {
    const limit = Number.parseInt(event.target.value, 10);

    if (!PAGE_SIZE_OPTIONS.some((pageSize) => pageSize === limit)) {
      return;
    }

    navigateSearch({
      ...search,
      limit,
      page: 1,
    });
  }

  function handleContentTypeChange(contentType: ContentTypeSelection | undefined) {
    navigateSearch({
      ...search,
      contentType,
      facets: sanitizeFacetSelections(search.facets, contentType),
      page: 1,
    });
  }

  function handleOrderChange(event: ChangeEvent<HTMLSelectElement>) {
    const option = ORDER_OPTIONS.find((orderOption) => orderOption.field === event.target.value);

    if (!option || (option.field === "relevance" && !search.query)) {
      return;
    }

    navigateSearch({
      ...search,
      descending: getDefaultDescending(option.field),
      order: option.field,
      page: 1,
    });
  }

  function handleDirectionToggle() {
    navigateSearch({
      ...search,
      descending: !search.descending,
      page: 1,
    });
  }

  function handleSizeApply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    navigateSearch({
      ...search,
      maxSize: parseSizeInput(sizeDraft.max),
      maxSizeUnit: sizeDraft.maxUnit,
      minSize: parseSizeInput(sizeDraft.min),
      minSizeUnit: sizeDraft.minUnit,
      page: 1,
    });
  }

  function handleSizeClear() {
    setSizeDraft({
      max: "",
      maxUnit: DEFAULT_SIZE_UNIT,
      min: "",
      minUnit: DEFAULT_SIZE_UNIT,
    });
    navigateSearch({
      ...search,
      maxSize: undefined,
      maxSizeUnit: DEFAULT_SIZE_UNIT,
      minSize: undefined,
      minSizeUnit: DEFAULT_SIZE_UNIT,
      page: 1,
    });
  }

  function handlePublishedChange(event: ChangeEvent<HTMLSelectElement>) {
    const value = event.target.value;

    if (value === PUBLISHED_CUSTOM_RANGE_VALUE) {
      setPublishedRangeOpen(true);
      setPublishedRangeDraft(getPublishedRangeDraft(search.publishedAt));
      navigateSearch({
        ...search,
        page: 1,
        publishedAt: undefined,
      });

      return;
    }

    setPublishedRangeOpen(false);
    setPublishedRangeDraft({ end: "", start: "" });
    navigateSearch({
      ...search,
      page: 1,
      publishedAt: isPublishedPreset(value) ? value : undefined,
    });
  }

  function handlePublishedRangeChange(field: keyof PublishedRangeDraft, value: string) {
    const nextDraft = {
      ...publishedRangeDraft,
      [field]: value,
    };
    setPublishedRangeDraft(nextDraft);

    if (!isPublishedRangeDraftComplete(nextDraft)) {
      return;
    }

    const publishedAt = formatPublishedRangeValue(nextDraft.start, nextDraft.end);

    if (!publishedAt) {
      return;
    }

    navigateSearch({
      ...search,
      page: 1,
      publishedAt,
    });
  }

  function handleFacetExpandedChange(key: TorrentSearchFacetKey, expanded: boolean) {
    setExpandedFacets((currentFacets) => {
      const nextFacets = new Set(currentFacets);

      if (expanded) {
        nextFacets.add(key);
      } else {
        nextFacets.delete(key);
      }

      return areFacetKeySetsEqual(currentFacets, nextFacets) ? currentFacets : nextFacets;
    });
  }

  function handleFacetValueChange(key: TorrentSearchFacetKey, value: string, checked: boolean) {
    navigateSearch({
      ...search,
      facets: updateFacetSelection(sanitizedFacetSelections, key, value, checked),
      page: 1,
    });
  }

  function handleFacetClear(key: TorrentSearchFacetKey) {
    const nextFacets: TorrentSearchFacetSelections = { ...sanitizedFacetSelections };
    delete nextFacets[key];

    navigateSearch({
      ...search,
      facets: nextFacets,
      page: 1,
    });
  }

  function handleResetFilters() {
    setExpandedFacets(new Set());
    setSizeDraft({
      max: "",
      maxUnit: DEFAULT_SIZE_UNIT,
      min: "",
      minUnit: DEFAULT_SIZE_UNIT,
    });
    setPublishedRangeOpen(false);
    setPublishedRangeDraft({ end: "", start: "" });
    navigateSearch({
      ...search,
      contentType: undefined,
      facets: {},
      maxSize: undefined,
      maxSizeUnit: DEFAULT_SIZE_UNIT,
      minSize: undefined,
      minSizeUnit: DEFAULT_SIZE_UNIT,
      page: 1,
      publishedAt: undefined,
    });
  }

  function handleCloseFilters() {
    const filters = filtersRef.current;
    filters?.removeAttribute("open");
    filters?.querySelector("summary")?.focus();
  }

  function setCurrentSelection(updater: (currentSelection: Set<string>) => Set<string>) {
    setSelection((currentSelection) => {
      const currentInfoHashes =
        currentSelection.searchParamsKey === searchParamsKey
          ? currentSelection.infoHashes
          : clearSelectionOnSearchParamsChange(
              currentSelection.infoHashes,
              currentSelection.searchParamsKey,
              searchParamsKey,
            );
      const nextInfoHashes = updater(currentInfoHashes);

      if (
        currentSelection.searchParamsKey === searchParamsKey &&
        nextInfoHashes === currentSelection.infoHashes
      ) {
        return currentSelection;
      }

      return {
        infoHashes: nextInfoHashes,
        searchParamsKey,
      };
    });
  }

  function handleResultSelectionChange(infoHash: string, checked: boolean) {
    setCurrentSelection((currentSelection) =>
      toggleInfoHashSelection(currentSelection, infoHash, checked),
    );
  }

  function handlePageSelectionToggle() {
    setCurrentSelection((currentSelection) =>
      togglePageSelection(currentSelection, pageInfoHashes),
    );
  }

  function handleClearSelection() {
    setCurrentSelection(() => new Set());
  }

  async function handleCopyHash(infoHash: string) {
    try {
      await navigator.clipboard.writeText(infoHash);
      notify({ message: t("toast.hashCopied") });
    } catch {
      notify({ message: t("toast.hashCopyFailed"), tone: "error" });
    }
  }

  async function handleCopyMagnet(magnetUri: string) {
    try {
      await navigator.clipboard.writeText(magnetUri);
      notify({
        message: t("toast.magnetCopied"),
      });
    } catch {
      notify({
        message: t("toast.magnetCopyFailed"),
        tone: "error",
      });
    }
  }

  const fetchMsRef = useRef<number | null>(null);
  const appLoadMs = useMemo(() => {
    const [nav] = performance.getEntriesByType("navigation") as PerformanceNavigationTiming[];

    if (!nav || nav.loadEventEnd <= 0) {
      return null;
    }

    return Math.round(nav.loadEventEnd - nav.startTime);
  }, []);
  const {
    data: searchData,
    error: searchError,
    isError: isSearchError,
    isFetching: isSearchFetching,
    isPending: isSearchPending,
    isSuccess: isSearchSuccess,
    refetch: refetchSearch,
  } = useQuery({
    enabled: effectiveMode === "torrents",
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const startedAt = performance.now();
      const response = await execute(
        TorrentContentSearchDocument,
        {
          cached: refresh.uncachedSearchKey === searchKey ? false : true,
          facets: getTorrentSearchFacets(search, activeFacetKeys),
          hasNextPage: true,
          limit: search.limit,
          orderBy: getTorrentSearchOrderBy(search),
          page: search.page,
          queryString: search.query || undefined,
          totalCount: true,
        },
        signal,
      );

      fetchMsRef.current = Math.round(performance.now() - startedAt);

      return response;
    },
    queryKey: ["torrentContentSearch", searchKey, refresh.nonce],
  });

  const result = searchData?.torrentContent.search;
  const pageItems = result?.items ?? EMPTY_SEARCH_ITEMS;
  const pageInfoHashes = useMemo(() => pageItems.map((item) => item.infoHash), [pageItems]);
  const pageSelection = useMemo(
    () => getPageSelectionState(selectedInfoHashes, pageInfoHashes),
    [pageInfoHashes, selectedInfoHashes],
  );
  const selectedItems = useMemo<TorrentActionItem[]>(() => {
    const items: TorrentActionItem[] = [];

    for (const item of pageItems) {
      if (selectedInfoHashes.has(item.infoHash)) {
        items.push({
          infoHash: item.infoHash,
          magnetUri: item.torrent.magnetUri,
        });
      }
    }

    return items;
  }, [pageItems, selectedInfoHashes]);
  const resultCount = result?.totalCount ?? 0;
  const isBrowse = search.query.length === 0;
  const totalCountLabel = result?.totalCountIsEstimate
    ? t("search.resultsCountEstimate", { count: resultCount })
    : t("search.resultsCount", { count: resultCount });
  const hasResults = Boolean(result && result.items.length > 0);
  const isBusy = isSearchPending || isSearchFetching;
  const contentTypeAggregations = result?.aggregations.contentType ?? [];
  const totalContentTypeCount =
    contentTypeAggregations.length > 0
      ? contentTypeAggregations.reduce((sum, agg) => sum + agg.count, 0)
      : resultCount;
  const totalContentTypeIsEstimate =
    contentTypeAggregations.length > 0
      ? contentTypeAggregations.some((agg) => agg.isEstimate)
      : Boolean(result?.totalCountIsEstimate);
  const contentTypeOptions = getContentTypeOptions(contentTypeAggregations, search.contentType, t);
  const hasSizeFilter = Boolean(search.minSize || search.maxSize);
  const hasDynamicFacetFilters = selectedFacetKeys.length > 0;
  const hasActiveFilters = Boolean(
    search.contentType || hasSizeFilter || search.publishedAt || hasDynamicFacetFilters,
  );
  const activeFilterCount =
    (hasSizeFilter ? 1 : 0) + (search.publishedAt ? 1 : 0) + selectedFacetKeys.length;
  const publishedRangeInvalid = isPublishedRangeDraftInvalid(publishedRangeDraft);
  const publishedSelectValue = publishedRangeOpen
    ? PUBLISHED_CUSTOM_RANGE_VALUE
    : (search.publishedAt ?? "");
  const orderOptions: ReactNode[] = [];

  for (const option of ORDER_OPTIONS) {
    if (option.field === "relevance" && !search.query) {
      continue;
    }

    orderOptions.push(
      <option key={option.field} value={option.field}>
        {t(`search.ordering.${option.field}`)}
      </option>,
    );
  }

  return (
    <section className={styles["root"]}>
      <h1 className={styles["srOnly"]}>{t("search.pageTitle")}</h1>
      {searchModesEnabled ? (
        <nav aria-label={t("search.modes.label")} className={styles["modeSwitch"]}>
          {SEARCH_MODES.map((mode) => (
            <button
              aria-pressed={effectiveMode === mode}
              data-active={effectiveMode === mode ? "true" : undefined}
              key={mode}
              onClick={() => handleSearchModeChange(mode)}
              type="button"
            >
              {t(`search.modes.${mode}`)}
            </button>
          ))}
        </nav>
      ) : null}
      {effectiveMode === "files" ? (
        <Suspense fallback={<ListSkeleton ariaLabel={t("fileSearch.loading")} rows={6} />}>
          <LazyFileSearchView />
        </Suspense>
      ) : null}
      {effectiveMode === "paths" ? (
        <Suspense fallback={<ListSkeleton ariaLabel={t("paths.loading")} rows={6} />}>
          <LazyPathBrowseView />
        </Suspense>
      ) : null}
      {effectiveMode === "torrents" ? (
        <>
          <form className={styles["searchForm"]} onSubmit={handleSubmit} role="search">
            <label className={styles["label"]} htmlFor="torrent-search">
              {t("search.inputLabel")}
            </label>
            <div className={styles["searchControl"]}>
              <input
                autoComplete="off"
                className={styles["input"]}
                id="torrent-search"
                onChange={handleQueryChange}
                placeholder={t("search.placeholder")}
                type="search"
                value={draftQuery}
              />
              <button className={styles["submit"]} type="submit">
                {t("search.submit")}
              </button>
            </div>
          </form>

          <div className={styles["primaryControls"]}>
            <section className={styles["filterBlock"]}>
              <h2>{t("search.contentType")}</h2>
              <div className={styles["chipRow"]}>
                <button
                  aria-pressed={!search.contentType}
                  className={styles["chip"]}
                  data-active={!search.contentType ? "true" : undefined}
                  onClick={() => handleContentTypeChange(undefined)}
                  type="button"
                >
                  <span>{t("search.contentTypeAll")}</span>
                  <small>
                    {formatIntEstimate(
                      totalContentTypeCount,
                      totalContentTypeIsEstimate,
                      2,
                      locale,
                    )}
                  </small>
                </button>
                {contentTypeOptions.map((option) => (
                  <button
                    aria-pressed={search.contentType === option.value}
                    className={styles["chip"]}
                    data-active={search.contentType === option.value ? "true" : undefined}
                    key={option.value}
                    onClick={() => handleContentTypeChange(option.value)}
                    type="button"
                  >
                    <span>{option.label}</span>
                    <small>{formatIntEstimate(option.count, option.isEstimate, 2, locale)}</small>
                  </button>
                ))}
              </div>
            </section>
            <section className={styles["filterBlock"]}>
              <h2>{t("search.sort")}</h2>
              <div className={styles["sortBar"]}>
                <label>
                  <span>{t("search.orderBy")}</span>
                  <select onChange={handleOrderChange} value={search.order}>
                    {orderOptions}
                  </select>
                </label>
                <button
                  aria-label={t("search.toggleSortDirection")}
                  className={styles["secondaryButton"]}
                  onClick={handleDirectionToggle}
                  type="button"
                >
                  {search.descending ? t("search.descending") : t("search.ascending")}
                </button>
              </div>
            </section>
            <SavedSearchControls params={searchParams} suggestedName={search.query} />
          </div>

          <details className={styles["filters"]} ref={filtersRef}>
            <summary
              aria-label={
                activeFilterCount > 0
                  ? t("search.filtersSummaryActive", { count: activeFilterCount })
                  : t("search.filtersSummary")
              }
            >
              {t("search.filtersSummary")}
              {activeFilterCount > 0 ? (
                <span className={styles["filterBadge"]}>{activeFilterCount}</span>
              ) : null}
            </summary>
            <button
              aria-label={t("search.closeFilters")}
              className={styles["filtersScrim"]}
              onClick={handleCloseFilters}
              tabIndex={-1}
              type="button"
            />
            <div className={styles["filtersBody"]}>
              <div className={styles["filtersSheetHeader"]}>
                <h2>{t("search.filtersSummary")}</h2>
                <button
                  className={styles["secondaryButton"]}
                  onClick={handleCloseFilters}
                  type="button"
                >
                  {t("search.closeFilters")}
                </button>
              </div>
              {hasActiveFilters ? (
                <div className={styles["filtersToolbar"]}>
                  <button
                    className={styles["secondaryButton"]}
                    onClick={handleResetFilters}
                    type="button"
                  >
                    {t("facets.reset")}
                  </button>
                </div>
              ) : null}

              <section className={styles["filterBlock"]}>
                <h2>{t("search.sizeFilter")}</h2>
                <form className={styles["sizeForm"]} onSubmit={handleSizeApply}>
                  <label>
                    <span>{t("search.minSize")}</span>
                    <input
                      min="0"
                      onChange={(event) =>
                        setSizeDraft((current) => ({ ...current, min: event.target.value }))
                      }
                      type="number"
                      value={sizeDraft.min}
                    />
                  </label>
                  <label>
                    <span>{t("search.minSizeUnit")}</span>
                    <select
                      onChange={(event) =>
                        setSizeDraft((current) => ({
                          ...current,
                          minUnit: event.target.value as SizeUnit,
                        }))
                      }
                      value={sizeDraft.minUnit}
                    >
                      {SIZE_UNITS.map((unit) => (
                        <option key={unit} value={unit}>
                          {t(`search.sizeUnits.${unit}`)}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    <span>{t("search.maxSize")}</span>
                    <input
                      min="0"
                      onChange={(event) =>
                        setSizeDraft((current) => ({ ...current, max: event.target.value }))
                      }
                      type="number"
                      value={sizeDraft.max}
                    />
                  </label>
                  <label>
                    <span>{t("search.maxSizeUnit")}</span>
                    <select
                      onChange={(event) =>
                        setSizeDraft((current) => ({
                          ...current,
                          maxUnit: event.target.value as SizeUnit,
                        }))
                      }
                      value={sizeDraft.maxUnit}
                    >
                      {SIZE_UNITS.map((unit) => (
                        <option key={unit} value={unit}>
                          {t(`search.sizeUnits.${unit}`)}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className={styles["filterActions"]}>
                    <button className={styles["submitSmall"]} type="submit">
                      {t("search.apply")}
                    </button>
                    <button
                      className={styles["secondaryButton"]}
                      disabled={!hasSizeFilter && !sizeDraft.min && !sizeDraft.max}
                      onClick={handleSizeClear}
                      type="button"
                    >
                      {t("search.clear")}
                    </button>
                  </div>
                </form>
              </section>

              <section className={styles["filterBlock"]}>
                <h2>{t("search.publishedFilter")}</h2>
                <label className={styles["publishedSelect"]}>
                  <span>{t("search.published")}</span>
                  <select onChange={handlePublishedChange} value={publishedSelectValue}>
                    <option value="">{t("search.publishedAny")}</option>
                    {PUBLISHED_PRESETS.map((preset) => (
                      <option key={preset.value} value={preset.value}>
                        {t(preset.labelKey)}
                      </option>
                    ))}
                    <option value={PUBLISHED_CUSTOM_RANGE_VALUE}>
                      {t("search.publishedCustomRange")}
                    </option>
                  </select>
                </label>
                {publishedRangeOpen ? (
                  <div className={styles["publishedRange"]}>
                    <label>
                      <span>{t("search.publishedRangeStart")}</span>
                      <input
                        aria-invalid={publishedRangeInvalid}
                        max={publishedRangeDraft.end || undefined}
                        onChange={(event) =>
                          handlePublishedRangeChange("start", event.target.value)
                        }
                        type="date"
                        value={publishedRangeDraft.start}
                      />
                    </label>
                    <label>
                      <span>{t("search.publishedRangeEnd")}</span>
                      <input
                        aria-invalid={publishedRangeInvalid}
                        min={publishedRangeDraft.start || undefined}
                        onChange={(event) => handlePublishedRangeChange("end", event.target.value)}
                        type="date"
                        value={publishedRangeDraft.end}
                      />
                    </label>
                    {publishedRangeInvalid ? (
                      <p className={styles["fieldError"]} role="alert">
                        {t("search.publishedRangeInvalid")}
                      </p>
                    ) : null}
                  </div>
                ) : null}
              </section>

              <div className={styles["facetGroups"]}>
                {relevantFacetKeys.map((key) => {
                  const selectedValues = sanitizedFacetSelections[key] ?? [];
                  const options = getDynamicFacetOptions(
                    result?.aggregations,
                    key,
                    selectedValues,
                    t,
                  );
                  const isExpanded = expandedFacets.has(key);
                  const hasSelections = selectedValues.length > 0;

                  return (
                    <details
                      className={styles["facetGroup"]}
                      data-selected={hasSelections ? "true" : undefined}
                      key={key}
                      onToggle={(event) => handleFacetExpandedChange(key, event.currentTarget.open)}
                      open={isExpanded}
                    >
                      <summary>
                        <span>{t(`facets.${key}`)}</span>
                        {hasSelections ? (
                          <small>{selectedValues.length.toLocaleString(locale)}</small>
                        ) : null}
                      </summary>
                      <div className={styles["facetGroupBody"]}>
                        {hasSelections ? (
                          <div className={styles["facetActions"]}>
                            <button
                              className={styles["secondaryButton"]}
                              onClick={() => handleFacetClear(key)}
                              type="button"
                            >
                              {t("facets.clear")}
                            </button>
                          </div>
                        ) : null}
                        {options.length ? (
                          <ul className={styles["facetOptionList"]}>
                            {options.map((option) => (
                              <li key={option.value}>
                                <label className={styles["facetOption"]}>
                                  <input
                                    checked={selectedValues.includes(option.value)}
                                    onChange={(event) =>
                                      handleFacetValueChange(
                                        key,
                                        option.value,
                                        event.target.checked,
                                      )
                                    }
                                    type="checkbox"
                                  />
                                  <span>{option.label}</span>
                                  <small>
                                    {formatIntEstimate(option.count, option.isEstimate, 2, locale)}
                                  </small>
                                </label>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className={styles["facetEmpty"]}>{t("facets.none")}</p>
                        )}
                      </div>
                    </details>
                  );
                })}
              </div>
            </div>
          </details>

          {isSearchPending ? (
            <div className={styles["resultsShell"]}>
              <ListSkeleton ariaLabel={t("search.loading")} rows={6} />
            </div>
          ) : null}

          {isSearchError ? (
            <div className={styles["resultsShell"]}>
              <QueryError error={searchError} onRetry={() => void refetchSearch()} />
            </div>
          ) : null}

          {isSearchSuccess ? (
            <div className={styles["resultsShell"]}>
              <div className={styles["resultsToolbar"]}>
                <div>
                  <p className={styles["resultsEyebrow"]}>
                    {isBrowse ? t("search.browseEyebrow") : search.query}
                  </p>
                  <h2 className={styles["resultsTitle"]}>{totalCountLabel}</h2>
                  {fetchMsRef.current !== null && result ? (
                    <p className={styles["latency"]}>
                      {t("search.fetchedIn", { ms: fetchMsRef.current.toLocaleString(locale) })}
                      {appLoadMs !== null
                        ? " \u00b7 " +
                          t("search.appLoadedIn", { ms: appLoadMs.toLocaleString(locale) })
                        : null}
                    </p>
                  ) : null}
                </div>
                <div className={styles["resultsActions"]}>
                  <label className={styles["selectAllControl"]}>
                    <IndeterminateCheckbox
                      aria-label={
                        pageSelection.allSelected
                          ? t("actions.deselectPage")
                          : t("actions.selectPage")
                      }
                      checked={pageSelection.allSelected}
                      disabled={!hasResults}
                      indeterminate={pageSelection.partiallySelected}
                      onChange={handlePageSelectionToggle}
                    />
                    <span>
                      {selectedItems.length > 0
                        ? t("actions.selectedCount", { count: selectedItems.length })
                        : t("actions.selectPage")}
                    </span>
                  </label>
                  <button
                    className={styles["secondaryButton"]}
                    disabled={isBusy}
                    onClick={handleRefresh}
                    type="button"
                  >
                    {t("search.refresh")}
                  </button>
                </div>
              </div>

              {selectedItems.length > 0 ? (
                <Suspense
                  fallback={
                    <div className={styles["bulkActionsFallback"]} role="status">
                      {t("actions.loading")}
                    </div>
                  }
                >
                  <LazyTorrentBulkActionsBar
                    items={selectedItems}
                    onClearSelection={handleClearSelection}
                  />
                </Suspense>
              ) : null}

              {hasResults && result ? (
                <ul className={styles["resultsList"]}>
                  {result.items.map((item: SearchItem) => {
                    const title = getResultTitle(item);
                    const torrentName = item.torrent.name.trim();
                    const showTorrentName = torrentName !== title;
                    const selected = selectedInfoHashes.has(item.infoHash);
                    const dhtSeenTooltip = getDhtSeenTooltip(item, {
                      count: t("search.dhtSeenCount"),
                      first: t("search.dhtFirstSeen"),
                      last: t("search.dhtLastSeen"),
                    });

                    return (
                      <li
                        className={styles["resultItem"]}
                        data-selected={selected ? "true" : undefined}
                        key={item.infoHash}
                      >
                        <label className={styles["resultSelect"]}>
                          <input
                            aria-label={
                              selected
                                ? t("actions.deselectResult", { title })
                                : t("actions.selectResult", { title })
                            }
                            checked={selected}
                            onChange={(event) =>
                              handleResultSelectionChange(item.infoHash, event.target.checked)
                            }
                            type="checkbox"
                          />
                        </label>
                        <div className={styles["resultMain"]}>
                          <h2>
                            <Link
                              className={styles["resultTitleLink"]}
                              params={{ infoHash: item.infoHash }}
                              to="/torrents/$infoHash"
                            >
                              {title}
                            </Link>
                          </h2>
                          {showTorrentName ? <p>{item.torrent.name}</p> : null}
                        </div>
                        <dl className={styles["resultMeta"]}>
                          <div>
                            <dt>{t("search.size")}</dt>
                            <dd>{formatFileSize(item.torrent.size)}</dd>
                          </div>
                          <div>
                            <dt>{t("search.published")}</dt>
                            <dd>
                              <time dateTime={item.publishedAt} title={item.publishedAt}>
                                {formatRelativeTime(item.publishedAt, undefined, locale)}
                              </time>
                            </dd>
                          </div>
                          {item.dhtLastSeenAt ? (
                            <div>
                              <dt>{t("search.dhtSeen")}</dt>
                              <dd title={dhtSeenTooltip}>
                                {t("search.dhtSeenSummary", {
                                  seenCount: item.dhtSeenCount.toLocaleString(locale),
                                  time: formatRelativeTime(item.dhtLastSeenAt, undefined, locale),
                                })}
                              </dd>
                            </div>
                          ) : null}
                          <div>
                            <dt>{t("search.peers")}</dt>
                            <dd>{getPeerLabel(item, locale)}</dd>
                          </div>
                          {item.torrent.filesCount ? (
                            <div>
                              <dt>{t("search.files")}</dt>
                              <dd>{item.torrent.filesCount.toLocaleString(locale)}</dd>
                            </div>
                          ) : null}
                          <div>
                            <dt>{t("search.infoHash")}</dt>
                            <dd>
                              <button
                                className={styles["hashButton"]}
                                onClick={() => void handleCopyHash(item.infoHash)}
                                title={item.infoHash}
                                type="button"
                              >
                                <code>{item.infoHash}</code>
                              </button>
                            </dd>
                          </div>
                        </dl>
                        <div className={styles["magnetActions"]}>
                          <a
                            aria-label={t("search.openMagnetLink", { title })}
                            className={styles["magnetLink"]}
                            href={item.torrent.magnetUri}
                            rel="noopener"
                          >
                            {t("search.magnet")}
                          </a>
                          <button
                            aria-label={t("search.copyMagnetLink", { title })}
                            className={styles["copyButton"]}
                            onClick={() => void handleCopyMagnet(item.torrent.magnetUri)}
                            type="button"
                          >
                            {t("search.copyMagnet")}
                          </button>
                        </div>
                      </li>
                    );
                  })}
                </ul>
              ) : (
                <div className={styles["emptyState"]}>
                  <h1>{isBrowse ? t("search.emptyTitle") : t("search.noResultsTitle")}</h1>
                  <p>{isBrowse ? t("search.emptyBody") : t("search.noResultsBody")}</p>
                </div>
              )}

              <div className={styles["pagination"]}>
                <label className={styles["pageSizeSelect"]}>
                  <span>{t("search.pageSize")}</span>
                  <select onChange={handlePageSizeChange} value={search.limit}>
                    {PAGE_SIZE_OPTIONS.map((pageSize) => (
                      <option key={pageSize} value={pageSize}>
                        {pageSize}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className={styles["secondaryButton"]}
                  disabled={search.page <= 1 || isBusy}
                  onClick={() => handlePageChange(search.page - 1)}
                  type="button"
                >
                  {t("search.previousPage")}
                </button>
                <span>{t("search.page", { page: search.page })}</span>
                <button
                  className={styles["secondaryButton"]}
                  disabled={!result?.hasNextPage || isBusy}
                  onClick={() => handlePageChange(search.page + 1)}
                  type="button"
                >
                  {t("search.nextPage")}
                </button>
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
