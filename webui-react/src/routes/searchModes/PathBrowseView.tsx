import type { ChangeEvent, FormEvent, KeyboardEvent } from "react";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ListSkeleton } from "../../components/ListSkeleton";
import { QueryError } from "../../components/QueryError";
import { execute } from "../../graphql/client";
import { CollapsePathsDocument, PathTypeaheadDocument } from "../../graphql/generated/graphql";
import type { CollapsePathsQuery } from "../../graphql/generated/graphql";
import { useDebouncedValue } from "../../utils/debounce";
import { getNextSuggestionIndex } from "../../utils/torrentMutationActions";
import {
  parseTorrentSearchParams,
  stringifyTorrentSearchParams,
  updateQuery,
} from "../searchParams";
import type { TorrentSearchState } from "../searchParams";
import searchStyles from "../SearchPage.module.css";
import styles from "./SearchModeViews.module.css";

export const PATH_TYPEAHEAD_DEBOUNCE_MS = 250;

type PathGroup = CollapsePathsQuery["torrentContent"]["collapsePaths"]["groups"][number];
type PathEntry =
  | {
      infoHashes: string[];
      kind: "directory";
      matchedPathCount: number;
      path: string;
      segment: string;
    }
  | {
      infoHashes: string[];
      kind: "leaf";
      path: string;
      segment: string;
    };

const EMPTY_PATH_GROUPS: PathGroup[] = [];

function getOffset(page: number, limit: number) {
  return Math.max(0, page - 1) * limit;
}

function normalizePrefix(value: string) {
  return value.trim().replace(/^\/+/, "");
}

function withTrailingSlash(value: string) {
  const prefix = normalizePrefix(value);

  if (!prefix) {
    return "";
  }

  return prefix.endsWith("/") ? prefix : `${prefix}/`;
}

function getRelativePath(path: string, prefix: string) {
  const normalizedPath = normalizePrefix(path);
  const normalizedPrefix = normalizePrefix(prefix);
  const prefixWithSlash = withTrailingSlash(normalizedPrefix);

  if (!prefixWithSlash) {
    return normalizedPath;
  }

  return normalizedPath.startsWith(prefixWithSlash)
    ? normalizedPath.slice(prefixWithSlash.length)
    : normalizedPath;
}

function getDirectoryPath(prefix: string, segment: string) {
  return `${withTrailingSlash(prefix)}${segment}/`;
}

function shortHash(infoHash: string) {
  return `${infoHash.slice(0, 8)}...`;
}

function getBreadcrumbs(prefix: string) {
  const trimmedPrefix = normalizePrefix(prefix).replace(/\/+$/, "");

  if (!trimmedPrefix) {
    return [];
  }

  const segments = trimmedPrefix.split("/").filter(Boolean);

  return segments.map((segment, index) => ({
    label: segment,
    prefix: `${segments.slice(0, index + 1).join("/")}/`,
  }));
}

function buildPathEntries(groups: readonly PathGroup[], prefix: string): PathEntry[] {
  const directories = new Map<
    string,
    {
      infoHashes: Set<string>;
      matchedPathCount: number;
      path: string;
      segment: string;
    }
  >();
  const leaves: PathEntry[] = [];

  for (const group of groups) {
    const relativePath = getRelativePath(group.path, prefix);
    const [segment, ...rest] = relativePath.split("/").filter(Boolean);

    if (!segment) {
      continue;
    }

    if (rest.length > 0) {
      const path = getDirectoryPath(prefix, segment);
      const directory = directories.get(path) ?? {
        infoHashes: new Set<string>(),
        matchedPathCount: 0,
        path,
        segment,
      };

      directory.matchedPathCount += 1;

      for (const infoHash of group.infoHashes) {
        directory.infoHashes.add(infoHash);
      }

      directories.set(path, directory);
      continue;
    }

    leaves.push({
      infoHashes: group.infoHashes,
      kind: "leaf",
      path: group.path,
      segment,
    });
  }

  return [
    ...Array.from(directories.values()).map<PathEntry>((directory) => ({
      infoHashes: Array.from(directory.infoHashes).sort(),
      kind: "directory",
      matchedPathCount: directory.matchedPathCount,
      path: directory.path,
      segment: directory.segment,
    })),
    ...leaves,
  ].sort((left, right) => {
    if (left.kind !== right.kind) {
      return left.kind === "directory" ? -1 : 1;
    }

    return left.segment.localeCompare(right.segment);
  });
}

export default function PathBrowseView() {
  const routeSearch = useSearch({ from: "/" });
  const search = useMemo(() => parseTorrentSearchParams(routeSearch), [routeSearch]);
  const [draftPrefix, setDraftPrefix] = useState(search.query);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(-1);
  const [suggestionsDismissed, setSuggestionsDismissed] = useState(false);
  const debouncedPrefix = useDebouncedValue(draftPrefix, PATH_TYPEAHEAD_DEBOUNCE_MS);
  const normalizedDebouncedPrefix = normalizePrefix(debouncedPrefix);
  const listboxId = useId();
  const fetchMsRef = useRef<number | null>(null);
  const navigate = useNavigate({ from: "/" });
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  const currentPrefix = normalizePrefix(search.query);
  const breadcrumbs = useMemo(() => getBreadcrumbs(currentPrefix), [currentPrefix]);

  useEffect(() => {
    setDraftPrefix(search.query);
  }, [search.query]);

  useEffect(() => {
    setActiveSuggestionIndex(-1);
    setSuggestionsDismissed(false);
  }, [normalizedDebouncedPrefix]);

  function navigateSearch(nextSearch: TorrentSearchState, replace = true) {
    void navigate({
      replace,
      resetScroll: false,
      search: stringifyTorrentSearchParams(nextSearch),
      to: "/",
    });
  }

  function navigateToPrefix(prefix: string, replace = true) {
    navigateSearch(updateQuery(search, prefix), replace);
  }

  function handlePrefixChange(event: ChangeEvent<HTMLInputElement>) {
    const nextValue = event.target.value;
    setDraftPrefix(nextValue);

    if (nextValue.trim() === "" && search.query) {
      navigateToPrefix("");
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    navigateToPrefix(normalizePrefix(draftPrefix));
  }

  function selectSuggestion(suggestion: string) {
    const nextPrefix = withTrailingSlash(suggestion);
    setDraftPrefix(nextPrefix);
    setActiveSuggestionIndex(-1);
    setSuggestionsDismissed(true);
    navigateToPrefix(nextPrefix);
  }

  const {
    data: typeaheadData,
    isError: isTypeaheadError,
    isFetching: isTypeaheadFetching,
  } = useQuery({
    enabled: normalizedDebouncedPrefix.length >= 3,
    queryFn: ({ signal }) =>
      execute(
        PathTypeaheadDocument,
        {
          input: {
            limit: 8,
            prefix: normalizedDebouncedPrefix,
          },
        },
        signal,
      ),
    queryKey: ["pathTypeahead", normalizedDebouncedPrefix],
  });

  const suggestions =
    normalizedDebouncedPrefix.length >= 3
      ? (typeaheadData?.torrentContent.pathTypeahead.suggestions ?? [])
      : [];
  const suggestionsOpen = suggestions.length > 0 && !suggestionsDismissed;
  const activeSuggestionId =
    activeSuggestionIndex >= 0 ? `${listboxId}-${activeSuggestionIndex}` : undefined;

  useEffect(() => {
    setActiveSuggestionIndex((currentIndex) =>
      currentIndex >= suggestions.length ? -1 : currentIndex,
    );
  }, [suggestions.length]);

  function handlePrefixKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      if (!suggestionsOpen) {
        return;
      }

      event.preventDefault();
      setActiveSuggestionIndex((currentIndex) =>
        getNextSuggestionIndex(currentIndex, suggestions.length, "down"),
      );
      return;
    }

    if (event.key === "ArrowUp") {
      if (!suggestionsOpen) {
        return;
      }

      event.preventDefault();
      setActiveSuggestionIndex((currentIndex) =>
        getNextSuggestionIndex(currentIndex, suggestions.length, "up"),
      );
      return;
    }

    if (event.key === "Enter") {
      if (event.nativeEvent.isComposing) {
        return;
      }

      const activeSuggestion = suggestions[activeSuggestionIndex];

      if (suggestionsOpen && activeSuggestion) {
        event.preventDefault();
        selectSuggestion(activeSuggestion);
      }

      return;
    }

    if (event.key === "Escape") {
      setActiveSuggestionIndex(-1);
      setSuggestionsDismissed(true);
    }
  }

  const collapseInput = useMemo(
    () => ({
      limit: search.limit,
      offset: getOffset(search.page, search.limit),
      queryString: currentPrefix,
    }),
    [currentPrefix, search.limit, search.page],
  );
  const {
    data: collapseData,
    error: collapseError,
    isError: isCollapseError,
    isFetching: isCollapseFetching,
    isPending: isCollapsePending,
    isSuccess: isCollapseSuccess,
    refetch: refetchCollapse,
  } = useQuery({
    enabled: currentPrefix.length > 0,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const startedAt = performance.now();
      const response = await execute(CollapsePathsDocument, { input: collapseInput }, signal);

      fetchMsRef.current = Math.round(performance.now() - startedAt);

      return response;
    },
    queryKey: ["collapsePaths", collapseInput],
  });

  const groups = collapseData?.torrentContent.collapsePaths.groups ?? EMPTY_PATH_GROUPS;
  const entries = useMemo(() => buildPathEntries(groups, currentPrefix), [currentPrefix, groups]);
  const isBusy = isCollapsePending || isCollapseFetching;

  return (
    <div className={styles["modeView"]}>
      <form className={searchStyles["searchForm"]} onSubmit={handleSubmit} role="search">
        <label className={searchStyles["label"]} htmlFor="path-search">
          {t("paths.inputLabel")}
        </label>
        <div className={styles["comboboxWrap"]}>
          <div className={searchStyles["searchControl"]}>
            <input
              aria-activedescendant={activeSuggestionId}
              aria-autocomplete="list"
              aria-controls={suggestionsOpen ? listboxId : undefined}
              aria-expanded={suggestionsOpen}
              aria-haspopup="listbox"
              autoComplete="off"
              className={searchStyles["input"]}
              id="path-search"
              onChange={handlePrefixChange}
              onFocus={() => setSuggestionsDismissed(false)}
              onKeyDown={handlePrefixKeyDown}
              placeholder={t("paths.placeholder")}
              role="combobox"
              type="search"
              value={draftPrefix}
            />
            <button className={searchStyles["submit"]} type="submit">
              {t("paths.browse")}
            </button>
          </div>
          {suggestionsOpen ? (
            <div
              aria-label={t("paths.suggestionsLabel")}
              className={styles["suggestions"]}
              id={listboxId}
              role="listbox"
            >
              {suggestions.map((suggestion, index) => (
                <button
                  aria-selected={index === activeSuggestionIndex}
                  className={styles["suggestion"]}
                  id={`${listboxId}-${index}`}
                  key={suggestion}
                  onClick={() => selectSuggestion(suggestion)}
                  onMouseDown={(event) => event.preventDefault()}
                  role="option"
                  type="button"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          ) : null}
        </div>
        {isTypeaheadError ? <p className={styles["muted"]}>{t("paths.suggestionsError")}</p> : null}
        {isTypeaheadFetching && normalizedDebouncedPrefix.length >= 3 ? (
          <p className={styles["muted"]}>{t("paths.suggestionsLoading")}</p>
        ) : null}
      </form>

      <nav aria-label={t("paths.breadcrumbLabel")}>
        <ol className={styles["breadcrumb"]}>
          <li>
            <button onClick={() => navigateToPrefix("")} type="button">
              {t("paths.root")}
            </button>
          </li>
          {breadcrumbs.map((crumb) => (
            <li key={crumb.prefix}>
              <button onClick={() => navigateToPrefix(crumb.prefix)} type="button">
                {crumb.label}
              </button>
            </li>
          ))}
        </ol>
      </nav>

      {!currentPrefix ? (
        <div className={styles["emptyState"]}>
          <h2>{t("paths.startTitle")}</h2>
          <p>{t("paths.startBody")}</p>
        </div>
      ) : null}

      {currentPrefix && isCollapsePending ? (
        <ListSkeleton ariaLabel={t("paths.loading")} rows={6} />
      ) : null}

      {currentPrefix && isCollapseError ? (
        <QueryError error={collapseError} onRetry={() => void refetchCollapse()} />
      ) : null}

      {currentPrefix && isCollapseSuccess ? (
        <>
          <div className={styles["toolbar"]}>
            <div className={styles["toolbarText"]}>
              <p className={styles["eyebrow"]}>{currentPrefix}</p>
              <h2 className={styles["title"]}>
                {t("paths.groupCount", { count: entries.length })}
              </h2>
              {fetchMsRef.current !== null ? (
                <p className={styles["latency"]}>
                  {t("search.fetchedIn", { ms: fetchMsRef.current.toLocaleString(locale) })}
                </p>
              ) : null}
            </div>
          </div>

          {entries.length > 0 ? (
            <ul className={styles["resultList"]}>
              {entries.map((entry) => (
                <li className={styles["pathRow"]} key={`${entry.kind}:${entry.path}`}>
                  <div className={styles["pathMain"]}>
                    {entry.kind === "directory" ? (
                      <button
                        className={styles["pathButton"]}
                        onClick={() => navigateToPrefix(entry.path)}
                        type="button"
                      >
                        {entry.segment}
                      </button>
                    ) : (
                      <span className={styles["pathText"]}>{entry.segment}</span>
                    )}
                    <span className={styles["badge"]}>
                      {entry.kind === "directory" ? t("paths.directory") : t("paths.file")}
                    </span>
                  </div>
                  <dl className={styles["metaGrid"]}>
                    <div>
                      <dt>{t("paths.torrents")}</dt>
                      <dd>{entry.infoHashes.length.toLocaleString(locale)}</dd>
                    </div>
                    <div>
                      <dt>{t("paths.path")}</dt>
                      <dd>{entry.path}</dd>
                    </div>
                    {entry.kind === "directory" ? (
                      <div>
                        <dt>{t("paths.matchingPaths")}</dt>
                        <dd>{entry.matchedPathCount.toLocaleString(locale)}</dd>
                      </div>
                    ) : null}
                  </dl>
                  {entry.kind === "leaf" ? (
                    <ul className={styles["torrentLinks"]} aria-label={t("paths.torrentLinks")}>
                      {entry.infoHashes.map((infoHash) => (
                        <li key={infoHash}>
                          <Link
                            className={styles["torrentLink"]}
                            params={{ infoHash }}
                            title={infoHash}
                            to="/torrents/$infoHash"
                          >
                            {shortHash(infoHash)}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </li>
              ))}
            </ul>
          ) : (
            <div className={styles["emptyState"]}>
              <h2>{t("paths.emptyTitle")}</h2>
              <p>{t("paths.emptyBody")}</p>
            </div>
          )}

          <div className={styles["pagination"]}>
            <button
              className={searchStyles["secondaryButton"]}
              disabled={search.page <= 1 || isBusy}
              onClick={() => navigateSearch({ ...search, page: search.page - 1 }, false)}
              type="button"
            >
              {t("search.previousPage")}
            </button>
            <span>{t("search.page", { page: search.page })}</span>
            <button
              className={searchStyles["secondaryButton"]}
              disabled={groups.length < search.limit || isBusy}
              onClick={() => navigateSearch({ ...search, page: search.page + 1 }, false)}
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
