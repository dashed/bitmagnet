import type { FormEvent } from "react";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../components/ListSkeleton";
import { QueryError } from "../components/QueryError";
import { execute } from "../graphql/client";
import { TorrentContentSearchDocument } from "../graphql/generated/graphql";
import type { TorrentContentSearchQuery } from "../graphql/generated/graphql";
import { formatFileSize } from "../utils/filesize";
import styles from "./SearchPage.module.css";

const PAGE_SIZE = 20;

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

export function SearchPage() {
  const [draftQuery, setDraftQuery] = useState("");
  const [request, setRequest] = useState<SearchRequest | null>(null);
  const { t } = useTranslation();

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const queryString = draftQuery.trim();

    if (!queryString) {
      setRequest(null);
      return;
    }

    setRequest((currentRequest) => ({
      cached: true,
      page: 1,
      queryString,
      requestId: (currentRequest?.requestId ?? 0) + 1,
    }));
  }

  function handleRefresh() {
    setRequest((currentRequest) => {
      if (!currentRequest) {
        return currentRequest;
      }

      return {
        ...currentRequest,
        cached: false,
        requestId: currentRequest.requestId + 1,
      };
    });
  }

  function handlePageChange(page: number) {
    setRequest((currentRequest) => {
      if (!currentRequest) {
        return currentRequest;
      }

      return {
        ...currentRequest,
        cached: true,
        page,
        requestId: currentRequest.requestId + 1,
      };
    });
  }

  const searchQuery = useQuery({
    enabled: request !== null,
    queryFn: () => {
      if (!request) {
        throw new Error("Search request is missing.");
      }

      return execute(TorrentContentSearchDocument, {
        input: {
          cached: request.cached,
          hasNextPage: true,
          limit: PAGE_SIZE,
          page: request.page,
          queryString: request.queryString,
          totalCount: true,
        },
      });
    },
    queryKey: ["torrentContentSearch", request],
  });

  const result = searchQuery.data?.torrentContent.search;
  const resultCount = result?.totalCount ?? 0;
  const totalCountLabel = result?.totalCountIsEstimate
    ? t("search.resultsCountEstimate", { count: resultCount })
    : t("search.resultsCount", { count: resultCount });
  const hasResults = Boolean(result && result.items.length > 0);
  const isBusy = searchQuery.isPending || searchQuery.isFetching;

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
            onChange={(event) => setDraftQuery(event.target.value)}
            placeholder={t("search.placeholder")}
            type="search"
            value={draftQuery}
          />
          <button className={styles["submit"]} type="submit">
            {t("search.submit")}
          </button>
        </div>
      </form>

      {!request ? (
        <div className={styles["emptyState"]}>
          <h1>{t("search.emptyTitle")}</h1>
          <p>{t("search.emptyBody")}</p>
        </div>
      ) : null}

      {request && searchQuery.isPending ? (
        <div className={styles["resultsShell"]}>
          <ListSkeleton ariaLabel={t("search.loading")} rows={6} />
        </div>
      ) : null}

      {request && searchQuery.isError ? (
        <div className={styles["resultsShell"]}>
          <QueryError error={searchQuery.error} onRetry={() => void searchQuery.refetch()} />
        </div>
      ) : null}

      {request && searchQuery.isSuccess ? (
        <div className={styles["resultsShell"]}>
          <div className={styles["resultsToolbar"]}>
            <div>
              <p className={styles["resultsEyebrow"]}>{request.queryString}</p>
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
              {result.items.map((item: SearchItem) => (
                <li className={styles["resultItem"]} key={item.infoHash}>
                  <div className={styles["resultMain"]}>
                    <h2>{getResultTitle(item)}</h2>
                    <p>{item.torrent.name}</p>
                  </div>
                  <dl className={styles["resultStats"]}>
                    <div>
                      <dt>{t("search.seeders")}</dt>
                      <dd>{getPeerCount(item.seeders).toLocaleString()}</dd>
                    </div>
                    <div>
                      <dt>{t("search.leechers")}</dt>
                      <dd>{getPeerCount(item.leechers).toLocaleString()}</dd>
                    </div>
                    <div>
                      <dt>{t("search.size")}</dt>
                      <dd>{formatFileSize(item.torrent.size)}</dd>
                    </div>
                  </dl>
                </li>
              ))}
            </ul>
          ) : (
            <div className={styles["emptyState"]}>
              <h1>{t("search.noResultsTitle")}</h1>
              <p>{t("search.noResultsBody")}</p>
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
