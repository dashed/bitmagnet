import type { ChangeEvent, ReactNode } from "react";
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { useRegisterCommands } from "../commands/CommandPaletteProvider";
import { ListSkeleton } from "../components/ListSkeleton";
import { QueryError } from "../components/QueryError";
import { TorrentMutationActions } from "../components/TorrentMutationActions";
import { useToast } from "../components/toast";
import { execute } from "../graphql/client";
import { TorrentDetailDocument, TorrentFilesDocument } from "../graphql/generated/graphql";
import type { FileType, TorrentDetailQuery, TorrentFilesQuery } from "../graphql/generated/graphql";
import { formatFileSize } from "../utils/filesize";
import { fuzzyMatch } from "../utils/fuzzy";
import { formatRelativeTime } from "../utils/relativeTime";
import { compareFileRows, type FileSort, type FileSortField } from "../utils/torrentFileSort";
import styles from "./TorrentDetailPage.module.css";

const INFO_HASH_PATTERN = /^[0-9a-fA-F]{40}$/;
const TMDB_POSTER_BASE_URL = "https://image.tmdb.org/t/p/w300/";
const FILES_PAGE_SIZE = 10;
const FILES_FETCH_LIMIT = 1_000;
const DEFAULT_FILE_SORT: FileSort = {
  direction: "asc",
  field: "index",
};
const DateTimeFormatter = Intl.DateTimeFormat;
const DATE_TIME_FORMATTERS = new Map<string, Intl.DateTimeFormat>();

type TorrentDetail = TorrentDetailQuery["torrentContent"]["search"]["items"][number];
type DetailTorrent = TorrentDetail["torrent"];
type DetailContent = NonNullable<TorrentDetail["content"]>;
type ContentAttribute = DetailContent["attributes"][number];
type ContentCollection = DetailContent["collections"][number];
type TorrentFile = TorrentFilesQuery["torrent"]["files"]["items"][number];
type AriaSort = "ascending" | "descending" | "none";
type FileRow = {
  fileType?: FileType | null;
  index: number;
  path: string;
  size: number;
};

function useParamInfoHash() {
  const params = useParams({ strict: false });
  const infoHash = params["infoHash"];

  return typeof infoHash === "string" ? infoHash : "";
}

function getDetailTitle(item: TorrentDetail) {
  return item.content?.title.trim() || item.title.trim() || item.torrent.name;
}

function getContentAttribute(
  attributes: ContentAttribute[] | undefined,
  key: string,
  source: string,
) {
  return attributes?.find((attribute) => attribute.key === key && attribute.source === source)
    ?.value;
}

function getPosterUrl(content: TorrentDetail["content"]) {
  const posterPath = getContentAttribute(content?.attributes, "poster_path", "tmdb");

  return posterPath ? `${TMDB_POSTER_BASE_URL}${posterPath}` : null;
}

function getGenres(collections: ContentCollection[] | undefined) {
  const genres: string[] = [];

  for (const collection of collections ?? []) {
    if (collection.type === "genre") {
      genres.push(collection.name);
    }
  }

  return genres.toSorted();
}

function getDateTimeFormatter(locale: string) {
  const cachedFormatter = DATE_TIME_FORMATTERS.get(locale);

  if (cachedFormatter) {
    return cachedFormatter;
  }

  const formatter = new DateTimeFormatter(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  });
  DATE_TIME_FORMATTERS.set(locale, formatter);

  return formatter;
}

function formatDateTime(value: string, locale: string) {
  const date = new Date(value);

  if (Number.isNaN(date.valueOf())) {
    return value;
  }

  return getDateTimeFormatter(locale).format(date);
}

function getPeerLabel(item: Pick<TorrentDetail, "leechers" | "seeders">, locale: string) {
  const seeders = item.seeders == null ? "?" : item.seeders.toLocaleString(locale);
  const leechers = item.leechers == null ? "?" : item.leechers.toLocaleString(locale);

  return `${seeders} / ${leechers}`;
}

function getDhtSeenTooltip(
  item: Pick<TorrentDetail, "dhtFirstSeenAt" | "dhtLastSeenAt" | "dhtSeenCount">,
  labels: { count: string; first: string; last: string },
  locale: string,
) {
  if (!item.dhtLastSeenAt) {
    return "";
  }

  return [
    item.dhtFirstSeenAt ? `${labels.first}: ${item.dhtFirstSeenAt}` : undefined,
    `${labels.last}: ${item.dhtLastSeenAt}`,
    `${labels.count}: ${item.dhtSeenCount.toLocaleString(locale)}`,
  ]
    .filter(Boolean)
    .join("\n");
}

function shouldQueryFiles(torrent: DetailTorrent) {
  return torrent.filesStatus === "multi" || torrent.filesStatus === "over_threshold";
}

function toFileRow(file: TorrentFile): FileRow {
  return {
    fileType: file.fileType,
    index: file.index,
    path: file.path,
    size: file.size,
  };
}

function getSingleFileRow(torrent: DetailTorrent): FileRow {
  return {
    fileType: torrent.fileType,
    index: 0,
    path: torrent.name,
    size: torrent.size,
  };
}

function getNextFileSort(current: FileSort, field: FileSortField): FileSort {
  if (current.field !== field) {
    return {
      direction: "asc",
      field,
    };
  }

  return {
    direction: current.direction === "asc" ? "desc" : "asc",
    field,
  };
}

function getAriaSort(sort: FileSort, field: FileSortField): AriaSort {
  if (sort.field !== field) {
    return "none";
  }

  return sort.direction === "asc" ? "ascending" : "descending";
}

function getDisplayedFileRows(rows: FileRow[], filterValue: string, sort: FileSort) {
  const filter = filterValue.trim();

  if (!filter) {
    return rows.toSorted((left, right) => compareFileRows(left, right, sort));
  }

  const scoredRows: Array<{ row: FileRow; score: number }> = [];

  for (const row of rows) {
    const score = fuzzyMatch(filter, row.path);

    if (score !== null) {
      scoredRows.push({ row, score });
    }
  }

  return scoredRows
    .sort((left, right) => right.score - left.score || compareFileRows(left.row, right.row, sort))
    .map(({ row }) => row);
}

async function writeClipboard(value: string) {
  await navigator.clipboard.writeText(value);
}

function NotFoundState() {
  const { t } = useTranslation();

  return (
    <div className="route-state" role="status">
      <h1>{t("detail.notFoundTitle")}</h1>
      <p>{t("detail.notFoundBody")}</p>
      <Link to="/">{t("detail.returnToSearch")}</Link>
    </div>
  );
}

function MetadataItem({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function TorrentFilesSection({ infoHash, torrent }: { infoHash: string; torrent: DetailTorrent }) {
  const [filterValue, setFilterValue] = useState("");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<FileSort>(DEFAULT_FILE_SORT);
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;
  const filesLimit = FILES_FETCH_LIMIT;
  const loadFiles = shouldQueryFiles(torrent);

  function handleFilterChange(value: string) {
    setFilterValue(value);
    setPage(1);
  }

  function handleSortChange(field: FileSortField) {
    setSort((current) => getNextFileSort(current, field));
    setPage(1);
  }

  const {
    data: filesData,
    error: filesError,
    isError: isFilesError,
    isPending: isFilesPending,
    refetch: refetchFiles,
  } = useQuery({
    enabled: loadFiles,
    queryFn: () =>
      execute(TorrentFilesDocument, {
        hasNextPage: false,
        infoHashes: [infoHash],
        limit: filesLimit,
        orderBy: [{ field: "index" }],
        totalCount: true,
      }),
    queryKey: ["torrentFiles", infoHash, filesLimit],
  });

  if (torrent.filesStatus === "no_info") {
    return (
      <section className={styles["section"]}>
        <h2>{t("detail.files")}</h2>
        <p className={styles["emptyText"]}>{t("detail.filesNoInfo")}</p>
      </section>
    );
  }

  if (torrent.filesStatus === "single") {
    return (
      <FilesList
        currentPage={1}
        enableControls={false}
        onPageChange={setPage}
        rows={[getSingleFileRow(torrent)]}
      />
    );
  }

  if (isFilesPending) {
    return (
      <section className={styles["section"]}>
        <h2>{t("detail.files")}</h2>
        <ListSkeleton ariaLabel={t("detail.filesLoading")} rows={3} />
      </section>
    );
  }

  if (isFilesError) {
    return (
      <section className={styles["section"]}>
        <h2>{t("detail.files")}</h2>
        <QueryError error={filesError} onRetry={() => void refetchFiles()} />
      </section>
    );
  }

  const filesResult = filesData?.torrent.files;

  if (!filesResult) {
    return (
      <section className={styles["section"]}>
        <h2>{t("detail.files")}</h2>
        <p className={styles["emptyText"]}>{t("detail.filesEmpty")}</p>
      </section>
    );
  }

  const rows = filesResult.items.map(toFileRow);
  const coveredFiles = Math.min(rows.length, FILES_FETCH_LIMIT).toLocaleString(locale);
  const totalFiles = filesResult.totalCount.toLocaleString(locale);
  const showLimitedFilesNote =
    torrent.filesStatus === "over_threshold" ||
    torrent.filesCount == null ||
    torrent.filesCount > FILES_FETCH_LIMIT ||
    filesResult.hasNextPage === true ||
    filesResult.totalCount > FILES_FETCH_LIMIT;

  return (
    <FilesList
      currentPage={page}
      enableControls
      filterValue={filterValue}
      note={
        showLimitedFilesNote
          ? t("detail.filesLimitedWindow", {
              shown: coveredFiles,
              total: totalFiles,
            })
          : null
      }
      onFilterChange={handleFilterChange}
      onPageChange={setPage}
      onSortChange={handleSortChange}
      rows={rows}
      sort={sort}
    />
  );
}

function FilesList({
  currentPage,
  enableControls = true,
  filterValue = "",
  note,
  onFilterChange,
  onPageChange,
  onSortChange,
  rows,
  sort = DEFAULT_FILE_SORT,
}: {
  currentPage: number;
  enableControls?: boolean;
  filterValue?: string;
  note?: string | null;
  onFilterChange?: (value: string) => void;
  onPageChange: (page: number) => void;
  onSortChange?: (field: FileSortField) => void;
  rows: FileRow[];
  sort?: FileSort;
}) {
  const { t } = useTranslation();
  const filterIsActive = filterValue.trim().length > 0;
  const displayedRows = useMemo(
    () => (enableControls ? getDisplayedFileRows(rows, filterValue, sort) : rows),
    [enableControls, filterValue, rows, sort],
  );
  const totalPages = Math.max(1, Math.ceil(displayedRows.length / FILES_PAGE_SIZE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const pageStart = (safeCurrentPage - 1) * FILES_PAGE_SIZE;
  const visibleRows = displayedRows.slice(pageStart, pageStart + FILES_PAGE_SIZE);
  const showPagination = displayedRows.length > FILES_PAGE_SIZE;
  const sortColumns: Array<{ field: FileSortField; label: string }> = [
    { field: "index", label: t("detail.fileIndex") },
    { field: "path", label: t("detail.filePath") },
    { field: "type", label: t("detail.fileType") },
    { field: "size", label: t("detail.fileSize") },
  ];

  function handleFilterInputChange(event: ChangeEvent<HTMLInputElement>) {
    onFilterChange?.(event.target.value);
  }

  function handleClearFilter() {
    onFilterChange?.("");
  }

  return (
    <section className={styles["section"]}>
      <div className={styles["sectionHeader"]}>
        <h2>{t("detail.files")}</h2>
        <span>{t("detail.filesCount", { count: rows.length })}</span>
      </div>

      {note ? <p className={styles["note"]}>{note}</p> : null}

      {enableControls ? (
        <div className={styles["filesToolbar"]}>
          <label className={styles["fileFilter"]} htmlFor="torrent-files-filter">
            <span>{t("detail.fileFilterLabel")}</span>
            <div className={styles["fileFilterControl"]}>
              <input
                autoComplete="off"
                id="torrent-files-filter"
                onChange={handleFilterInputChange}
                placeholder={t("detail.fileFilterPlaceholder")}
                type="search"
                value={filterValue}
              />
              <button
                className={styles["secondaryButton"]}
                disabled={!filterValue}
                onClick={handleClearFilter}
                type="button"
              >
                {t("search.clear")}
              </button>
            </div>
          </label>

          {filterIsActive ? (
            <p className={styles["note"]}>
              {t("detail.filesMatchCount", {
                count: displayedRows.length,
                total: rows.length,
              })}
            </p>
          ) : null}
        </div>
      ) : null}

      {!enableControls && visibleRows.length > 0 ? (
        <ol className={styles["filesList"]} start={pageStart + 1}>
          {visibleRows.map((file) => (
            <li className={styles["fileRow"]} key={`${file.index}-${file.path}`}>
              <span className={styles["fileIndex"]}>
                {t("detail.fileIndexValue", { index: file.index })}
              </span>
              <span className={styles["filePath"]}>{file.path}</span>
              <dl className={styles["fileMeta"]}>
                <div>
                  <dt>{t("detail.fileType")}</dt>
                  <dd>{t(`fileTypes.${file.fileType ?? "unknown"}`)}</dd>
                </div>
                <div>
                  <dt>{t("detail.fileSize")}</dt>
                  <dd>{formatFileSize(file.size)}</dd>
                </div>
              </dl>
            </li>
          ))}
        </ol>
      ) : null}

      {enableControls && visibleRows.length > 0 ? (
        <div className={styles["filesTableScroll"]}>
          <table className={styles["filesTable"]}>
            <thead>
              <tr>
                {sortColumns.map((column) => (
                  <th aria-sort={getAriaSort(sort, column.field)} key={column.field} scope="col">
                    <button
                      className={styles["fileSortButton"]}
                      onClick={() => onSortChange?.(column.field)}
                      type="button"
                    >
                      <span>{column.label}</span>
                      {sort.field === column.field ? (
                        <span className={styles["sortIndicator"]}>
                          {t(
                            sort.direction === "asc"
                              ? "detail.fileSortAscending"
                              : "detail.fileSortDescending",
                          )}
                        </span>
                      ) : null}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((file) => (
                <tr key={`${file.index}-${file.path}`}>
                  <td className={styles["fileIndexCell"]}>
                    {t("detail.fileIndexValue", { index: file.index })}
                  </td>
                  <td className={styles["filePathCell"]}>{file.path}</td>
                  <td>{t(`fileTypes.${file.fileType ?? "unknown"}`)}</td>
                  <td>{formatFileSize(file.size)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : enableControls ? (
        <p className={styles["emptyText"]}>
          {t(filterIsActive ? "detail.filesFilterEmpty" : "detail.filesEmpty")}
        </p>
      ) : null}

      {showPagination ? (
        <div className={styles["pagination"]}>
          <button
            className={styles["secondaryButton"]}
            disabled={safeCurrentPage <= 1}
            onClick={() => onPageChange(safeCurrentPage - 1)}
            type="button"
          >
            {t("search.previousPage")}
          </button>
          <span>{t("detail.filesPage", { page: safeCurrentPage, totalPages })}</span>
          <button
            className={styles["secondaryButton"]}
            disabled={safeCurrentPage >= totalPages}
            onClick={() => onPageChange(safeCurrentPage + 1)}
            type="button"
          >
            {t("search.nextPage")}
          </button>
        </div>
      ) : null}
    </section>
  );
}

export default function TorrentDetailPage() {
  const rawInfoHash = useParamInfoHash();
  const infoHash = rawInfoHash.toLowerCase();
  const isValidInfoHash = INFO_HASH_PATTERN.test(rawInfoHash);
  const navigate = useNavigate({ from: "/torrents/$infoHash" });
  const notify = useToast();
  const { i18n, t } = useTranslation();
  const locale = i18n.resolvedLanguage ?? i18n.language;

  const {
    data: detailData,
    error: detailError,
    isError: isDetailError,
    isPending: isDetailPending,
    refetch: refetchDetail,
  } = useQuery({
    enabled: isValidInfoHash,
    queryFn: () =>
      execute(TorrentDetailDocument, {
        infoHashes: [infoHash],
        limit: 1,
      }),
    queryKey: ["torrentDetail", infoHash],
  });
  const item = detailData?.torrentContent.search.items[0];
  const commandInfoHash = item?.infoHash;
  const commandMagnetUri = item?.torrent.magnetUri;

  async function handleCopy(value: string, successMessage: string, failureMessage: string) {
    try {
      await writeClipboard(value);
      notify({ message: successMessage });
    } catch {
      notify({ message: failureMessage, tone: "error" });
    }
  }

  useRegisterCommands(
    () => {
      if (!commandInfoHash || !commandMagnetUri) {
        return [];
      }

      return [
        {
          group: "actions",
          id: "detail-copy-magnet",
          perform: () =>
            handleCopy(
              commandMagnetUri,
              t("toast.magnetCopied"),
              t("toast.magnetCopyFailed"),
            ),
          title: t("palette.copyMagnet"),
        },
        {
          group: "actions",
          id: "detail-copy-info-hash",
          perform: () =>
            handleCopy(
              commandInfoHash,
              t("toast.infoHashCopied"),
              t("toast.infoHashCopyFailed"),
            ),
          title: t("palette.copyHash"),
        },
      ];
    },
    [commandInfoHash, commandMagnetUri, notify, t],
  );

  if (!isValidInfoHash) {
    return <NotFoundState />;
  }

  if (isDetailPending) {
    return <ListSkeleton ariaLabel={t("detail.loading")} rows={6} />;
  }

  if (isDetailError) {
    return <QueryError error={detailError} onRetry={() => void refetchDetail()} />;
  }

  if (!item) {
    return <NotFoundState />;
  }

  const title = getDetailTitle(item);
  const posterUrl = getPosterUrl(item.content);
  const genres = getGenres(item.content?.collections);
  const releaseDate = item.content?.releaseDate ?? item.content?.releaseYear?.toString();
  const originalLanguageId = item.content?.originalLanguage?.id;
  const showTorrentName = item.torrent.name !== title;
  const rating =
    item.content?.voteAverage == null
      ? null
      : `${item.content.voteAverage.toLocaleString(locale)} / 10`;

  return (
    <article className={styles["root"]}>
      <div className={styles["backLink"]}>
        <Link to="/">{t("detail.returnToSearch")}</Link>
      </div>

      <header className={styles["header"]}>
        {posterUrl ? (
          <img
            alt={t("detail.posterAlt", { title })}
            className={styles["poster"]}
            height={450}
            loading="lazy"
            src={posterUrl}
            width={300}
          />
        ) : null}

        <div className={styles["summary"]}>
          <p className={styles["eyebrow"]}>
            {item.contentType ? t(`contentTypes.${item.contentType}`) : t("detail.unknown")}
          </p>
          <h1>{title}</h1>
          {showTorrentName ? <p className={styles["torrentName"]}>{item.torrent.name}</p> : null}

          <div className={styles["actions"]}>
            <a
              aria-label={t("search.openMagnetLink", { title })}
              className={styles["primaryLink"]}
              href={item.torrent.magnetUri}
              rel="noopener noreferrer"
            >
              {t("search.magnet")}
            </a>
            <button
              className={styles["secondaryButton"]}
              onClick={() =>
                void handleCopy(
                  item.torrent.magnetUri,
                  t("toast.magnetCopied"),
                  t("toast.magnetCopyFailed"),
                )
              }
              type="button"
            >
              {t("search.copyMagnet")}
            </button>
          </div>

          <dl className={styles["metadata"]}>
            <MetadataItem label={t("detail.size")} value={formatFileSize(item.torrent.size)} />
            <MetadataItem
              label={t("detail.published")}
              value={
                <time dateTime={item.publishedAt} title={item.publishedAt}>
                  {formatRelativeTime(item.publishedAt, undefined, locale)}
                </time>
              }
            />
            <MetadataItem label={t("detail.peers")} value={getPeerLabel(item, locale)} />
            {item.dhtLastSeenAt ? (
              <MetadataItem
                label={t("detail.dhtSeen")}
                value={
                  <span
                    title={getDhtSeenTooltip(
                      item,
                      {
                        count: t("detail.dhtSeenCount"),
                        first: t("detail.dhtFirstSeen"),
                        last: t("detail.dhtLastSeen"),
                      },
                      locale,
                    )}
                  >
                    {t("detail.dhtSeenSummary", {
                      seenCount: item.dhtSeenCount.toLocaleString(locale),
                      time: formatRelativeTime(item.dhtLastSeenAt, undefined, locale),
                    })}
                  </span>
                }
              />
            ) : null}
            {releaseDate ? (
              <MetadataItem label={t("detail.releaseDate")} value={releaseDate} />
            ) : null}
            {item.episodes?.label ? (
              <MetadataItem label={t("detail.episodes")} value={item.episodes.label} />
            ) : null}
            {item.content?.originalTitle ? (
              <MetadataItem label={t("detail.originalTitle")} value={item.content.originalTitle} />
            ) : null}
          </dl>
        </div>
      </header>

      <section className={styles["section"]}>
        <h2>{t("detail.infoHash")}</h2>
        <div className={styles["hashRow"]}>
          <code>{item.infoHash}</code>
          <button
            className={styles["secondaryButton"]}
            onClick={() =>
              void handleCopy(
                item.infoHash,
                t("toast.infoHashCopied"),
                t("toast.infoHashCopyFailed"),
              )
            }
            type="button"
          >
            {t("detail.copyInfoHash")}
          </button>
        </div>
      </section>

      <section className={styles["section"]}>
        <h2>{t("actions.title")}</h2>
        <TorrentMutationActions
          infoHashes={[item.infoHash]}
          onDeleteSuccess={() => void navigate({ to: "/" })}
        />
      </section>

      {item.torrent.sources.length > 0 ? (
        <section className={styles["section"]}>
          <h2>{t("detail.sources")}</h2>
          <ul className={styles["sourcesList"]}>
            {item.torrent.sources.map((source) => (
              <li key={source.key}>
                <strong>{source.name}</strong>
                <dl>
                  <div>
                    <dt>{t("detail.seen")}</dt>
                    <dd>{t("detail.sourceSeenCount", { count: source.seenCount })}</dd>
                  </div>
                  <div>
                    <dt>{t("detail.firstSeen")}</dt>
                    <dd>
                      <time dateTime={source.firstSeenAt} title={source.firstSeenAt}>
                        {formatDateTime(source.firstSeenAt, locale)}
                      </time>
                    </dd>
                  </div>
                  <div>
                    <dt>{t("detail.lastSeen")}</dt>
                    <dd>
                      <time dateTime={source.lastSeenAt} title={source.lastSeenAt}>
                        {formatDateTime(source.lastSeenAt, locale)}
                      </time>
                    </dd>
                  </div>
                </dl>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {item.languages?.length ? (
        <section className={styles["section"]}>
          <h2>{t("detail.languages")}</h2>
          <ul className={styles["inlineList"]}>
            {item.languages.map((language) => (
              <li key={language.id}>
                {language.name}
                {language.id === originalLanguageId ? ` ${t("detail.originalMarker")}` : ""}
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {item.content?.overview ? (
        <section className={styles["section"]}>
          <h2>{t("detail.overview")}</h2>
          <p className={styles["overview"]}>{item.content.overview}</p>
        </section>
      ) : null}

      {genres.length > 0 || rating ? (
        <section className={styles["section"]}>
          <h2>{t("detail.content")}</h2>
          <dl className={styles["metadata"]}>
            {genres.length > 0 ? (
              <MetadataItem label={t("detail.genres")} value={genres.join(", ")} />
            ) : null}
            {rating ? (
              <MetadataItem
                label={t("detail.rating")}
                value={
                  item.content?.voteCount == null
                    ? rating
                    : `${rating} (${t("detail.ratingVotes", { count: item.content.voteCount })})`
                }
              />
            ) : null}
          </dl>
        </section>
      ) : null}

      {item.content?.externalLinks.length ? (
        <section className={styles["section"]}>
          <h2>{t("detail.externalLinks")}</h2>
          <ul className={styles["externalLinks"]}>
            {item.content.externalLinks.map((link) => (
              <li key={link.metadataSource.key}>
                <a href={link.url} rel="noopener noreferrer" target="_blank">
                  {link.metadataSource.name}
                </a>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <TorrentFilesSection key={infoHash} infoHash={infoHash} torrent={item.torrent} />
    </article>
  );
}
