import type { ChangeEvent, FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../components/ListSkeleton";
import { QueryError } from "../components/QueryError";
import { useToast } from "../components/toast";
import { execute } from "../graphql/client";
import { TorrentContentSearchDocument } from "../graphql/generated/graphql";
import type { ContentTypeAgg, TorrentContentSearchQuery } from "../graphql/generated/graphql";
import { formatFileSize } from "../utils/filesize";
import { formatIntEstimate } from "../utils/intEstimate";
import { formatRelativeTime } from "../utils/relativeTime";
import {
  DEFAULT_SIZE_UNIT,
  ORDER_OPTIONS,
  PUBLISHED_PRESETS,
  SIZE_UNITS,
  getDefaultDescending,
  getTorrentSearchFacets,
  getTorrentSearchOrderBy,
  parseTorrentSearchParams,
  stringifyTorrentSearchParams,
  updateQuery,
} from "./searchParams";
import type { ContentTypeSelection, SizeUnit, TorrentSearchState } from "./searchParams";
import styles from "./SearchPage.module.css";

type SearchResult = TorrentContentSearchQuery["torrentContent"]["search"];
type SearchItem = SearchResult["items"][number];
type SizeDraft = {
  max: string;
  maxUnit: SizeUnit;
  min: string;
  minUnit: SizeUnit;
};
type ContentTypeOption = {
  count: number;
  isEstimate: boolean;
  label: string;
  value: ContentTypeSelection;
};

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

function isPublishedPreset(value: string): value is (typeof PUBLISHED_PRESETS)[number]["value"] {
  return PUBLISHED_PRESETS.some((preset) => preset.value === value);
}

function getDhtSeenTooltip(
  item: SearchItem,
  labels: { count: string; first: string; last: string },
) {
  if (!item.dhtLastSeenAt) {
    return "";
  }

  return [
    item.dhtFirstSeenAt ? `${labels.first}: ${item.dhtFirstSeenAt}` : undefined,
    `${labels.last}: ${item.dhtLastSeenAt}`,
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

export function SearchPage() {
  const routeSearch = useSearch({ from: "/" });
  const search = useMemo(() => parseTorrentSearchParams(routeSearch), [routeSearch]);
  const searchParams = useMemo(() => stringifyTorrentSearchParams(search), [search]);
  const searchKey = useMemo(() => JSON.stringify(searchParams), [searchParams]);
  const [draftQuery, setDraftQuery] = useState(search.query);
  const [sizeDraft, setSizeDraft] = useState<SizeDraft>(() => getSizeDraft(search));
  const [refresh, setRefresh] = useState<{ nonce: number; uncachedSearchKey: string | null }>({
    nonce: 0,
    uncachedSearchKey: null,
  });
  const navigate = useNavigate({ from: "/" });
  const notify = useToast();
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;

  useEffect(() => {
    setDraftQuery(search.query);
  }, [search.query]);

  useEffect(() => {
    setSizeDraft(getSizeDraft(search));
  }, [search]);

  function navigateSearch(nextSearch: TorrentSearchState, replace = true) {
    void navigate({
      replace,
      resetScroll: false,
      search: stringifyTorrentSearchParams(nextSearch),
      to: "/",
    });
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

  function handleContentTypeChange(contentType: ContentTypeSelection | undefined) {
    navigateSearch({
      ...search,
      contentType,
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
    navigateSearch({
      ...search,
      page: 1,
      publishedAt: isPublishedPreset(value) ? value : undefined,
    });
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
  const searchQuery = useQuery({
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const startedAt = performance.now();
      const response = await execute(TorrentContentSearchDocument, {
        cached: refresh.uncachedSearchKey === searchKey ? false : true,
        facets: getTorrentSearchFacets(search),
        hasNextPage: true,
        limit: search.limit,
        orderBy: getTorrentSearchOrderBy(search),
        page: search.page,
        queryString: search.query || undefined,
        totalCount: true,
      });

      fetchMsRef.current = Math.round(performance.now() - startedAt);

      return response;
    },
    queryKey: ["torrentContentSearch", searchKey, refresh.nonce],
  });

  const result = searchQuery.data?.torrentContent.search;
  const resultCount = result?.totalCount ?? 0;
  const isBrowse = search.query.length === 0;
  const totalCountLabel = result?.totalCountIsEstimate
    ? t("search.resultsCountEstimate", { count: resultCount })
    : t("search.resultsCount", { count: resultCount });
  const hasResults = Boolean(result && result.items.length > 0);
  const isBusy = searchQuery.isPending || searchQuery.isFetching;
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

  return (
    <section className={styles["root"]}>
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

      <details className={styles["filters"]} open>
        <summary>{t("search.filtersSummary")}</summary>
        <div className={styles["filtersBody"]}>
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
                  {formatIntEstimate(totalContentTypeCount, totalContentTypeIsEstimate, 2, locale)}
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
                  {ORDER_OPTIONS.filter(
                    (option) => option.field !== "relevance" || search.query,
                  ).map((option) => (
                    <option key={option.field} value={option.field}>
                      {t(`search.ordering.${option.field}`)}
                    </option>
                  ))}
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
              <select onChange={handlePublishedChange} value={search.publishedAt ?? ""}>
                <option value="">{t("search.publishedAny")}</option>
                {PUBLISHED_PRESETS.map((preset) => (
                  <option key={preset.value} value={preset.value}>
                    {t(preset.labelKey)}
                  </option>
                ))}
              </select>
            </label>
          </section>
        </div>
      </details>

      {searchQuery.isPending ? (
        <div className={styles["resultsShell"]}>
          <ListSkeleton ariaLabel={t("search.loading")} rows={6} />
        </div>
      ) : null}

      {searchQuery.isError ? (
        <div className={styles["resultsShell"]}>
          <QueryError error={searchQuery.error} onRetry={() => void searchQuery.refetch()} />
        </div>
      ) : null}

      {searchQuery.isSuccess ? (
        <div className={styles["resultsShell"]}>
          <div className={styles["resultsToolbar"]}>
            <div>
              <p className={styles["resultsEyebrow"]}>
                {isBrowse ? t("search.browseEyebrow") : search.query}
              </p>
              <h1 className={styles["resultsTitle"]}>{totalCountLabel}</h1>
              {fetchMsRef.current !== null && result ? (
                <p className={styles["latency"]}>
                  {t("search.fetchedIn", { ms: fetchMsRef.current.toLocaleString(locale) })}
                  {appLoadMs !== null
                    ? " \u00b7 " + t("search.appLoadedIn", { ms: appLoadMs.toLocaleString(locale) })
                    : null}
                </p>
              ) : null}
            </div>
            <button
              className={styles["secondaryButton"]}
              disabled={isBusy}
              onClick={handleRefresh}
              type="button"
            >
              {t("search.refresh")}
            </button>
          </div>

          {hasResults && result ? (
            <ul className={styles["resultsList"]}>
              {result.items.map((item: SearchItem) => {
                const title = getResultTitle(item);
                const torrentName = item.torrent.name.trim();
                const showTorrentName = torrentName !== title;
                const dhtSeenTooltip = getDhtSeenTooltip(item, {
                  count: t("search.dhtSeenCount"),
                  first: t("search.dhtFirstSeen"),
                  last: t("search.dhtLastSeen"),
                });

                return (
                  <li className={styles["resultItem"]} key={item.infoHash}>
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
                            <code>{item.infoHash.slice(0, 8)}\u2026</code>
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
    </section>
  );
}
