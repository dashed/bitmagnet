import type { ChangeEvent, FormEvent } from "react";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../components/ListSkeleton";
import { QueryError } from "../components/QueryError";
import { useToast } from "../components/toast";
import { execute } from "../graphql/client";
import { TorrentContentSearchDocument } from "../graphql/generated/graphql";
import type {
  TorrentContentOrderByInput,
  TorrentContentSearchQuery,
} from "../graphql/generated/graphql";
import { formatFileSize } from "../utils/filesize";
import { formatRelativeTime } from "../utils/relativeTime";
import styles from "./SearchPage.module.css";

const PAGE_SIZE = 20;
const BROWSE_ORDER_BY: TorrentContentOrderByInput[] = [
  {
    descending: true,
    field: "published_at",
  },
];
const SEARCH_ORDER_BY: TorrentContentOrderByInput[] = [
  {
    descending: true,
    field: "relevance",
  },
];

type SearchRequest = {
  cached: boolean;
  page: number;
  queryString: string;
  requestId: number;
};

type SearchResult = TorrentContentSearchQuery["torrentContent"]["search"];
type SearchItem = SearchResult["items"][number];

function getPeerCount(value: number | null | undefined) {
  return value ?? 0;
}

function getResultTitle(item: SearchItem) {
  return item.title.trim() || item.torrent.name;
}

function getSearchOrderBy(queryString: string) {
  return queryString ? SEARCH_ORDER_BY : BROWSE_ORDER_BY;
}

function getPeerLabel(item: SearchItem) {
  return `${getPeerCount(item.seeders).toLocaleString()} / ${getPeerCount(
    item.leechers,
  ).toLocaleString()}`;
}

export function SearchPage() {
  const [draftQuery, setDraftQuery] = useState("");
  const [request, setRequest] = useState<SearchRequest>({
    cached: true,
    page: 1,
    queryString: "",
    requestId: 0,
  });
  const notify = useToast();
  const { i18n, t } = useTranslation();

  function handleQueryChange(event: ChangeEvent<HTMLInputElement>) {
    const nextValue = event.target.value;
    setDraftQuery(nextValue);

    if (nextValue.trim() === "" && request.queryString) {
      setRequest((currentRequest) => ({
        cached: true,
        page: 1,
        queryString: "",
        requestId: currentRequest.requestId + 1,
      }));
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const queryString = draftQuery.trim();

    setRequest((currentRequest) => ({
      cached: true,
      page: 1,
      queryString,
      requestId: currentRequest.requestId + 1,
    }));
  }

  function handleRefresh() {
    setRequest((currentRequest) => ({
      ...currentRequest,
      cached: false,
      requestId: currentRequest.requestId + 1,
    }));
  }

  function handlePageChange(page: number) {
    setRequest((currentRequest) => ({
      ...currentRequest,
      cached: true,
      page,
      requestId: currentRequest.requestId + 1,
    }));
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

  const searchQuery = useQuery({
    queryFn: () => {
      return execute(TorrentContentSearchDocument, {
        cached: request.cached,
        hasNextPage: true,
        limit: PAGE_SIZE,
        orderBy: getSearchOrderBy(request.queryString),
        page: request.page,
        queryString: request.queryString || undefined,
        totalCount: true,
      });
    },
    queryKey: ["torrentContentSearch", request],
  });

  const result = searchQuery.data?.torrentContent.search;
  const resultCount = result?.totalCount ?? 0;
  const isBrowse = request.queryString.length === 0;
  const totalCountLabel = result?.totalCountIsEstimate
    ? t("search.resultsCountEstimate", { count: resultCount })
    : t("search.resultsCount", { count: resultCount });
  const hasResults = Boolean(result && result.items.length > 0);
  const isBusy = searchQuery.isPending || searchQuery.isFetching;
  const locale = i18n.resolvedLanguage ?? i18n.language;

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
                {isBrowse ? t("search.browseEyebrow") : request.queryString}
              </p>
              <h1 className={styles["resultsTitle"]}>{totalCountLabel}</h1>
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

                return (
                  <li className={styles["resultItem"]} key={item.infoHash}>
                    <div className={styles["resultMain"]}>
                      <h2>{title}</h2>
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
                      <div>
                        <dt>{t("search.peers")}</dt>
                        <dd>{getPeerLabel(item)}</dd>
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
              disabled={request.page <= 1 || isBusy}
              onClick={() => handlePageChange(request.page - 1)}
              type="button"
            >
              {t("search.previousPage")}
            </button>
            <span>{t("search.page", { page: request.page })}</span>
            <button
              className={styles["secondaryButton"]}
              disabled={!result?.hasNextPage || isBusy}
              onClick={() => handlePageChange(request.page + 1)}
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
