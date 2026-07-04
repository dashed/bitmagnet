import type { ChangeEvent, FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../../components/ListSkeleton";
import { QueryError } from "../../components/QueryError";
import { execute } from "../../graphql/client";
import { FileSearchDocument } from "../../graphql/generated/graphql";
import type { FileSearchInput, FileSearchQuery } from "../../graphql/generated/graphql";
import { formatFileSize } from "../../utils/filesize";
import {
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
    totalCount: true,
  };
}

export default function FileSearchView() {
  const routeSearch = useSearch({ from: "/" });
  const search = useMemo(() => parseTorrentSearchParams(routeSearch), [routeSearch]);
  const [draftQuery, setDraftQuery] = useState(search.query);
  const fetchMsRef = useRef<number | null>(null);
  const navigate = useNavigate({ from: "/" });
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

  const { data, error, isError, isFetching, isPending, isSuccess, refetch } = useQuery({
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
  const isBusy = isPending || isFetching;

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
              {items.map((item) => (
                <li className={styles["fileRow"]} key={`${item.infoHash}:${item.index}`}>
                  <div className={styles["fileMain"]}>
                    <Link
                      className={styles["torrentLink"]}
                      params={{ infoHash: item.infoHash }}
                      to="/torrents/$infoHash"
                    >
                      {item.path}
                    </Link>
                    <span className={styles["badge"]}>
                      {item.extension || t("fileSearch.noExtension")}
                    </span>
                  </div>
                  <dl className={styles["metaGrid"]}>
                    <div>
                      <dt>{t("fileSearch.size")}</dt>
                      <dd>{formatFileSize(item.size)}</dd>
                    </div>
                    <div>
                      <dt>{t("fileSearch.fileIndex")}</dt>
                      <dd>{t("fileSearch.fileIndexValue", { index: item.index })}</dd>
                    </div>
                    <div>
                      <dt>{t("fileSearch.torrent")}</dt>
                      <dd>
                        <Link
                          className={styles["torrentLink"]}
                          params={{ infoHash: item.infoHash }}
                          to="/torrents/$infoHash"
                        >
                          <code>{item.infoHash}</code>
                        </Link>
                      </dd>
                    </div>
                  </dl>
                </li>
              ))}
            </ul>
          ) : (
            <div className={styles["emptyState"]}>
              <h2>{t("fileSearch.emptyTitle")}</h2>
              <p>{t("fileSearch.emptyBody")}</p>
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
