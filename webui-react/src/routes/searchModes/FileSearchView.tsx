import type { ChangeEvent, FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../../components/ListSkeleton";
import { QueryError } from "../../components/QueryError";
import { useToast } from "../../components/toast";
import { execute } from "../../graphql/client";
import { FileSearchDocument } from "../../graphql/generated/graphql";
import type { FileSearchInput, FileSearchQuery } from "../../graphql/generated/graphql";
import { formatFileSize } from "../../utils/filesize";
import { formatRelativeTime } from "../../utils/relativeTime";
import {
  FILE_ORDER_OPTIONS,
  getDefaultDescending,
  getFileSearchSort,
  isFileOrderField,
  parseTorrentSearchParams,
  sizeToBytes,
  stringifyTorrentSearchParams,
  updateQuery,
} from "../searchParams";
import type { TorrentSearchState } from "../searchParams";
import searchStyles from "../SearchPage.module.css";
import styles from "./SearchModeViews.module.css";

type FileSearchItem = FileSearchQuery["torrentContent"]["fileSearch"]["items"][number];

const EMPTY_FILE_ITEMS: FileSearchItem[] = [];

function fileOptionRequiresQuery(option: (typeof FILE_ORDER_OPTIONS)[number]) {
  return "requiresQuery" in option && option.requiresQuery;
}

function getOffset(page: number, limit: number) {
  return Math.max(0, page - 1) * limit;
}

function getAppLoadMs() {
  const [nav] = performance.getEntriesByType("navigation") as PerformanceNavigationTiming[];

  if (!nav || nav.loadEventEnd <= 0) {
    return null;
  }

  return Math.round(nav.loadEventEnd - nav.startTime);
}

function getFileSearchInput(search: TorrentSearchState): FileSearchInput {
  return {
    limit: search.limit,
    maxSize: sizeToBytes(search.maxSize, search.maxSizeUnit),
    minSize: sizeToBytes(search.minSize, search.minSizeUnit),
    offset: getOffset(search.page, search.limit),
    query: search.query || undefined,
    sort: getFileSearchSort(search),
    totalCount: true,
  };
}

function getPeerCount(value: number | null | undefined) {
  return value ?? 0;
}

function getTorrentTitle(item: FileSearchItem) {
  return item.torrentContent.title.trim() || item.torrentContent.torrent.name;
}

export default function FileSearchView() {
  const routeSearch = useSearch({ from: "/" });
  const search = useMemo(() => parseTorrentSearchParams(routeSearch), [routeSearch]);
  const [draftQuery, setDraftQuery] = useState(search.query);
  const fetchMsRef = useRef<number | null>(null);
  const navigate = useNavigate({ from: "/" });
  const notify = useToast();
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  const appLoadMs = useMemo(() => getAppLoadMs(), []);
  const input = useMemo(() => getFileSearchInput(search), [search]);

  useEffect(() => {
    setDraftQuery(search.query);
  }, [search.query]);

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

  function handlePageChange(page: number) {
    navigateSearch({ ...search, page }, false);
  }

  function handleOrderChange(event: ChangeEvent<HTMLSelectElement>) {
    const field = event.target.value;
    const option = FILE_ORDER_OPTIONS.find((candidate) => candidate.field === field);

    if (!option || !isFileOrderField(field) || (fileOptionRequiresQuery(option) && !search.query)) {
      return;
    }

    navigateSearch({
      ...search,
      descending: getDefaultDescending(field),
      order: field,
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

  async function handleCopyMagnet(magnetUri: string) {
    try {
      await navigator.clipboard.writeText(magnetUri);
      notify({ message: t("toast.magnetCopied") });
    } catch {
      notify({ message: t("toast.magnetCopyFailed"), tone: "error" });
    }
  }

  const hasQuery = search.query.trim().length > 0;
  const { data, error, isError, isFetching, isPending, isSuccess, refetch } = useQuery({
    enabled: hasQuery,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const startedAt = performance.now();
      const response = await execute(FileSearchDocument, { input }, signal);

      fetchMsRef.current = Math.round(performance.now() - startedAt);

      return response;
    },
    queryKey: ["fileSearch", input],
  });

  const result = data?.torrentContent.fileSearch;
  const items = result?.items ?? EMPTY_FILE_ITEMS;
  const hasResults = items.length > 0;
  const isBusy = hasQuery && (isPending || isFetching);
  const selectedOrder = isFileOrderField(search.order) ? search.order : "size";

  return (
    <div className={styles["modeView"]}>
      <form className={searchStyles["searchForm"]} onSubmit={handleSubmit} role="search">
        <label className={searchStyles["label"]} htmlFor="file-search">
          {t("fileSearch.inputLabel")}
        </label>
        <div className={searchStyles["searchControl"]}>
          <input
            autoComplete="off"
            className={searchStyles["input"]}
            id="file-search"
            onChange={handleQueryChange}
            placeholder={t("fileSearch.placeholder")}
            type="search"
            value={draftQuery}
          />
          <button className={searchStyles["submit"]} type="submit">
            {t("search.submit")}
          </button>
        </div>
      </form>

      <section className={styles["sortBar"]} aria-label={t("fileSearch.sort")}>
        <label>
          <span>{t("fileSearch.orderBy")}</span>
          <select onChange={handleOrderChange} value={selectedOrder}>
            {FILE_ORDER_OPTIONS.map((option) => (
              <option
                disabled={fileOptionRequiresQuery(option) && !search.query}
                key={option.field}
                value={option.field}
              >
                {t(`search.ordering.${option.field}`)}
              </option>
            ))}
          </select>
        </label>
        <button
          aria-label={t("search.toggleSortDirection")}
          className={searchStyles["secondaryButton"]}
          onClick={handleDirectionToggle}
          type="button"
        >
          {search.descending ? t("search.descending") : t("search.ascending")}
        </button>
      </section>

      {isPending ? <ListSkeleton ariaLabel={t("fileSearch.loading")} rows={6} /> : null}

      {isError ? <QueryError error={error} onRetry={() => void refetch()} /> : null}

      {isSuccess && result ? (
        <>
          <div className={styles["toolbar"]}>
            <div className={styles["toolbarText"]}>
              <p className={styles["eyebrow"]}>
                {search.query ? search.query : t("fileSearch.browseEyebrow")}
              </p>
              <h2 className={styles["title"]}>
                {t("fileSearch.resultsCount", { count: result.totalCount })}
              </h2>
              {fetchMsRef.current !== null ? (
                <p className={styles["latency"]}>
                  {t("search.fetchedIn", { ms: fetchMsRef.current.toLocaleString(locale) })}
                  {appLoadMs !== null
                    ? " \u00b7 " + t("search.appLoadedIn", { ms: appLoadMs.toLocaleString(locale) })
                    : null}
                </p>
              ) : null}
            </div>
          </div>

          {hasResults ? (
            <ul className={styles["resultList"]}>
              {items.map((item) => {
                const title = getTorrentTitle(item);
                const seeders = getPeerCount(item.torrentContent.seeders).toLocaleString(locale);
                const leechers = getPeerCount(item.torrentContent.leechers).toLocaleString(locale);
                const updated = formatRelativeTime(
                  item.torrentContent.updatedAt,
                  undefined,
                  locale,
                );
                const lastSeen = item.torrentContent.dhtLastSeenAt
                  ? formatRelativeTime(item.torrentContent.dhtLastSeenAt, undefined, locale)
                  : null;

                return (
                  <li className={styles["fileRow"]} key={`${item.infoHash}:${item.index}`}>
                    <div className={styles["fileMain"]}>
                      <div className={styles["fileText"]}>
                        <p className={styles["pathText"]}>{item.path}</p>
                        <Link
                          className={styles["torrentLink"]}
                          params={{ infoHash: item.torrentContent.infoHash }}
                          to="/torrents/$infoHash"
                        >
                          {title}
                        </Link>
                      </div>
                      <div className={styles["fileActions"]}>
                        <span className={styles["badge"]}>
                          {item.extension || t("fileSearch.noExtension")}
                        </span>
                        <button
                          aria-label={t("search.copyMagnetLink", { title })}
                          className={styles["copyButton"]}
                          onClick={() =>
                            void handleCopyMagnet(item.torrentContent.torrent.magnetUri)
                          }
                          type="button"
                        >
                          {t("search.copyMagnet")}
                        </button>
                      </div>
                    </div>
                    <p className={styles["metaLine"]}>
                      <span>{formatFileSize(item.size)}</span>
                      <span>{t("fileSearch.peerSummary", { leechers, seeders })}</span>
                      <span>{t("fileSearch.updated", { time: updated })}</span>
                      <span>
                        {lastSeen
                          ? t("fileSearch.lastSeen", { time: lastSeen })
                          : t("fileSearch.lastSeenUnknown")}
                      </span>
                    </p>
                  </li>
                );
              })}
            </ul>
          ) : (
            <div className={styles["emptyState"]}>
              {hasQuery ? (
                <>
                  <h2>{t("fileSearch.emptyTitle")}</h2>
                  <p>{t("fileSearch.emptyBody")}</p>
                </>
              ) : (
                <>
                  <h2>{t("fileSearch.startTitle")}</h2>
                  <p>{t("fileSearch.startBody")}</p>
                </>
              )}
            </div>
          )}

          <div className={styles["pagination"]}>
            <button
              className={searchStyles["secondaryButton"]}
              disabled={search.page <= 1 || isBusy}
              onClick={() => handlePageChange(search.page - 1)}
              type="button"
            >
              {t("search.previousPage")}
            </button>
            <span>{t("search.page", { page: search.page })}</span>
            <button
              className={searchStyles["secondaryButton"]}
              disabled={!result.hasNextPage || isBusy}
              onClick={() => handlePageChange(search.page + 1)}
              type="button"
            >
              {t("search.nextPage")}
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}
