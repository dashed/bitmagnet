import type { SearchMiddleware } from "@tanstack/react-router";

import type {
  ContentType,
  FileSearchSortInput,
  FileType,
  Language,
  TorrentContentFacetsInput,
  TorrentContentOrderByField,
  TorrentContentOrderByInput,
  VideoResolution,
  VideoSource,
} from "../graphql/generated/graphql";

export const DEFAULT_SEARCH_LIMIT = 20;
export const DEFAULT_SIZE_UNIT = "MiB";
export const INFO_HASH_PATTERN = /^[0-9a-fA-F]{40}$/;
export const PAGE_SIZE_OPTIONS = [10, 20, 50, 100] as const;
export const SEARCH_MODES = ["torrents", "files", "paths"] as const;

export const SIZE_UNITS = ["KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB"] as const;
export type SizeUnit = (typeof SIZE_UNITS)[number];
export type SearchMode = (typeof SEARCH_MODES)[number];

export const CONTENT_TYPE_VALUES = [
  "audiobook",
  "comic",
  "ebook",
  "game",
  "movie",
  "music",
  "software",
  "tv_show",
  "xxx",
] as const satisfies readonly ContentType[];

export type ContentTypeSelection = ContentType | "null";

export const TORRENT_SEARCH_FACET_KEYS = [
  "torrent_source",
  "torrent_tag",
  "file_type",
  "language",
  "genre",
  "video_resolution",
  "video_source",
] as const;

export type TorrentSearchFacetKey = (typeof TORRENT_SEARCH_FACET_KEYS)[number];
export type TorrentSearchFacetSelections = Partial<Record<TorrentSearchFacetKey, string[]>>;

const FILE_TYPE_VALUES = [
  "archive",
  "audio",
  "data",
  "document",
  "image",
  "software",
  "subtitles",
  "video",
] as const satisfies readonly FileType[];

const LANGUAGE_VALUES = [
  "af",
  "ar",
  "az",
  "be",
  "bg",
  "bs",
  "ca",
  "ce",
  "co",
  "cs",
  "cy",
  "da",
  "de",
  "el",
  "en",
  "es",
  "et",
  "eu",
  "fa",
  "fi",
  "fr",
  "he",
  "hi",
  "hr",
  "hu",
  "hy",
  "id",
  "is",
  "it",
  "ja",
  "ka",
  "ko",
  "ku",
  "lt",
  "lv",
  "mi",
  "mk",
  "ml",
  "mn",
  "ms",
  "mt",
  "nl",
  "no",
  "pl",
  "pt",
  "ro",
  "ru",
  "sa",
  "sk",
  "sl",
  "sm",
  "so",
  "sr",
  "sv",
  "ta",
  "th",
  "tr",
  "uk",
  "vi",
  "yi",
  "zh",
  "zu",
] as const satisfies readonly Language[];

const VIDEO_RESOLUTION_VALUES = [
  "V360p",
  "V480p",
  "V540p",
  "V576p",
  "V720p",
  "V1080p",
  "V1440p",
  "V2160p",
  "V4320p",
] as const satisfies readonly VideoResolution[];

const VIDEO_SOURCE_VALUES = [
  "CAM",
  "TELECINE",
  "TELESYNC",
  "WORKPRINT",
  "DVD",
  "TV",
  "WEBDL",
  "WEBRip",
  "BluRay",
] as const satisfies readonly VideoSource[];

const FACET_CONTENT_TYPES: Partial<Record<TorrentSearchFacetKey, readonly ContentType[]>> = {
  genre: ["movie", "tv_show"],
  video_resolution: ["movie", "tv_show", "xxx"],
  video_source: ["movie", "tv_show", "xxx"],
};

export const ORDER_OPTIONS = [
  { defaultDescending: true, field: "relevance" },
  { defaultDescending: false, field: "name" },
  { defaultDescending: true, field: "published_at" },
  { defaultDescending: true, field: "updated_at" },
  { defaultDescending: true, field: "size" },
  { defaultDescending: true, field: "files_count" },
  { defaultDescending: true, field: "seeders" },
  { defaultDescending: true, field: "leechers" },
] as const satisfies ReadonlyArray<{
  defaultDescending: boolean;
  field: TorrentContentOrderByField;
}>;

export const FILE_ORDER_OPTIONS = [
  { defaultDescending: true, field: "size" },
  { defaultDescending: true, field: "last_seen", requiresQuery: true },
  { defaultDescending: true, field: "seeders", requiresQuery: true },
  { defaultDescending: true, field: "published_at", requiresQuery: true },
  { defaultDescending: true, field: "updated_at", requiresQuery: true },
  { defaultDescending: false, field: "path" },
] as const;

export type FileSearchOrderField = (typeof FILE_ORDER_OPTIONS)[number]["field"];
export type SearchOrderField = TorrentContentOrderByField | FileSearchOrderField;

export const PUBLISHED_PRESETS = [
  { labelKey: "search.publishedLastDay", value: "24h" },
  { labelKey: "search.publishedLastWeek", value: "7d" },
  { labelKey: "search.publishedLastMonth", value: "30d" },
  { labelKey: "search.publishedLastThreeMonths", value: "90d" },
  { labelKey: "search.publishedLastYear", value: "last year" },
] as const;

export type PublishedPreset = (typeof PUBLISHED_PRESETS)[number]["value"];
export type PublishedFilter = string;

const PUBLISHED_SPECIAL_VALUES = [
  "today",
  "yesterday",
  "this week",
  "last week",
  "this month",
  "last month",
  "this year",
  "last year",
] as const;

const PUBLISHED_MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

const PUBLISHED_MONTH_INDEX = new Map<string, number>(
  PUBLISHED_MONTHS.map((month, index) => [month, index + 1] as const),
);

type ParsedPublishedDate = {
  inputValue: string;
  sortKey: string;
};

type SearchInput = Record<string, unknown>;

export type TorrentSearchUrlParams = {
  content_type?: ContentTypeSelection;
  desc?: 0 | 1;
  limit?: number;
  max_size?: number;
  max_size_unit?: SizeUnit;
  min_size?: number;
  min_size_unit?: SizeUnit;
  mode?: Exclude<SearchMode, "torrents">;
  order?: SearchOrderField;
  page?: number;
  published_at?: PublishedFilter;
  query?: string;
} & Partial<Record<TorrentSearchFacetKey, string>>;

export type TorrentSearchState = {
  contentType?: ContentTypeSelection;
  descending: boolean;
  facets: TorrentSearchFacetSelections;
  limit: number;
  maxSize?: number;
  maxSizeUnit: SizeUnit;
  minSize?: number;
  minSizeUnit: SizeUnit;
  mode: SearchMode;
  order: SearchOrderField;
  page: number;
  publishedAt?: PublishedFilter;
  query: string;
};

export type LegacyTorrentSearchNormalization =
  | {
      infoHash: string;
      kind: "detail";
    }
  | {
      kind: "search";
      search: TorrentSearchUrlParams;
    }
  | {
      kind: "none";
    };

function searchInput(input: unknown): SearchInput {
  return (input && typeof input === "object" ? input : {}) as SearchInput;
}

function firstValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    const values = value as unknown[];

    return values[0];
  }

  return value;
}

function safeDecode(value: string) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function stringValue(value: unknown) {
  const candidate = firstValue(value);

  if (typeof candidate !== "string") {
    return undefined;
  }

  const decoded = safeDecode(candidate).trim();

  return decoded || undefined;
}

function hasSearchParam(params: SearchInput, key: string) {
  return Object.prototype.hasOwnProperty.call(params, key);
}

function legacyTorrentValue(params: SearchInput) {
  const candidate = firstValue(params["torrent"]);

  if (typeof candidate !== "string") {
    return "";
  }

  return safeDecode(candidate).trim();
}

function stringListValue(value: unknown) {
  const candidates = Array.isArray(value) ? value : [value];
  const values = candidates.flatMap((candidate) => {
    if (typeof candidate !== "string") {
      return [];
    }

    const decoded = safeDecode(candidate).trim();

    if (!decoded) {
      return [];
    }

    return decoded.split(",").flatMap((part) => {
      const value = safeDecode(part).trim();

      return value ? [value] : [];
    });
  });

  return Array.from(new Set(values)).sort();
}

function integerValue(value: unknown, minimum: number) {
  const candidate = firstValue(value);

  if (typeof candidate === "number" && Number.isInteger(candidate) && candidate >= minimum) {
    return candidate;
  }

  if (typeof candidate === "string" && /^\d+$/.test(candidate)) {
    const parsed = Number.parseInt(candidate, 10);

    return parsed >= minimum ? parsed : undefined;
  }

  return undefined;
}

function booleanValue(value: unknown) {
  const candidate = firstValue(value);

  if (candidate === true || candidate === 1 || candidate === "1" || candidate === "true") {
    return true;
  }

  if (candidate === false || candidate === 0 || candidate === "0" || candidate === "false") {
    return false;
  }

  return undefined;
}

function contentTypeValue(value: unknown): ContentTypeSelection | undefined {
  const candidate = stringValue(value);

  if (candidate === "null") {
    return candidate;
  }

  return CONTENT_TYPE_VALUES.includes(candidate as ContentType)
    ? (candidate as ContentType)
    : undefined;
}

function isTorrentOrderField(value: string): value is TorrentContentOrderByField {
  return ORDER_OPTIONS.some((option) => option.field === value);
}

export function isFileOrderField(value: string): value is FileSearchOrderField {
  return FILE_ORDER_OPTIONS.some((option) => option.field === value);
}

function fileOrderRequiresQuery(field: FileSearchOrderField) {
  const option = FILE_ORDER_OPTIONS.find((candidate) => candidate.field === field);

  return option ? "requiresQuery" in option && option.requiresQuery : false;
}

function isFileOrderAllowedForQuery(field: FileSearchOrderField, query: string) {
  return !fileOrderRequiresQuery(field) || query.length > 0;
}

function isOrderAllowedForMode(mode: SearchMode, field: SearchOrderField, query: string) {
  if (mode === "files") {
    return isFileOrderField(field) && isFileOrderAllowedForQuery(field, query);
  }

  return isTorrentOrderField(field) && (field !== "relevance" || query.length > 0);
}

function orderValue(value: unknown, mode: SearchMode, query: string): SearchOrderField | undefined {
  const candidate = stringValue(value);

  if (!candidate) {
    return undefined;
  }

  if (mode === "files") {
    return isFileOrderField(candidate) && isFileOrderAllowedForQuery(candidate, query)
      ? candidate
      : undefined;
  }

  return isTorrentOrderField(candidate) && (candidate !== "relevance" || query)
    ? candidate
    : undefined;
}

function sizeUnitValue(value: unknown): SizeUnit {
  const candidate = stringValue(value);

  return SIZE_UNITS.includes(candidate as SizeUnit) ? (candidate as SizeUnit) : DEFAULT_SIZE_UNIT;
}

function searchModeValue(value: unknown): SearchMode {
  const candidate = stringValue(value);

  return SEARCH_MODES.includes(candidate as SearchMode) ? (candidate as SearchMode) : "torrents";
}

export function isPublishedPreset(value: string): value is PublishedPreset {
  return PUBLISHED_PRESETS.some((preset) => preset.value === value);
}

function isPublishedSpecialValue(value: string) {
  return PUBLISHED_SPECIAL_VALUES.includes(value as (typeof PUBLISHED_SPECIAL_VALUES)[number]);
}

function padDatePart(value: number) {
  return value.toString().padStart(2, "0");
}

function isValidDateParts(year: number, month: number, day: number) {
  const date = new Date(Date.UTC(year, month - 1, day));

  return (
    date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day
  );
}

function isValidTimeParts(hour: number, minute: number, second: number) {
  return hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59 && second >= 0 && second <= 59;
}

function parsedPublishedDate(
  year: number,
  month: number,
  day: number,
  hour = 0,
  minute = 0,
  second = 0,
): ParsedPublishedDate | undefined {
  if (!isValidDateParts(year, month, day) || !isValidTimeParts(hour, minute, second)) {
    return undefined;
  }

  const inputValue = `${year}-${padDatePart(month)}-${padDatePart(day)}`;
  const timeValue = `${padDatePart(hour)}:${padDatePart(minute)}:${padDatePart(second)}`;

  return {
    inputValue,
    sortKey: `${inputValue}T${timeValue}Z`,
  };
}

function parsePublishedDate(value: string): ParsedPublishedDate | undefined {
  let match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (match) {
    return parsedPublishedDate(Number(match[1]), Number(match[2]), Number(match[3]));
  }

  match = /^(\d{4})\/(\d{2})\/(\d{2})$/.exec(value);
  if (match) {
    return parsedPublishedDate(Number(match[1]), Number(match[2]), Number(match[3]));
  }

  match = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(value);
  if (match) {
    return parsedPublishedDate(Number(match[3]), Number(match[1]), Number(match[2]));
  }

  match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$/.exec(value);
  if (match) {
    return parsedPublishedDate(
      Number(match[1]),
      Number(match[2]),
      Number(match[3]),
      Number(match[4]),
      Number(match[5]),
      Number(match[6]),
    );
  }

  match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(value);
  if (match) {
    return parsedPublishedDate(
      Number(match[1]),
      Number(match[2]),
      Number(match[3]),
      Number(match[4]),
      Number(match[5]),
      Number(match[6]),
    );
  }

  match = /^(\d{1,2})-([A-Z][a-z]{2})-(\d{4})$/.exec(value);
  if (match) {
    const month = PUBLISHED_MONTH_INDEX.get(match[2]);

    return month ? parsedPublishedDate(Number(match[3]), month, Number(match[1])) : undefined;
  }

  match = /^([A-Z][a-z]{2}) (\d{1,2}), (\d{4})$/.exec(value);
  if (match) {
    const month = PUBLISHED_MONTH_INDEX.get(match[1]);

    return month ? parsedPublishedDate(Number(match[3]), month, Number(match[2])) : undefined;
  }

  return undefined;
}

export function getPublishedRangeInputValues(
  value: string | undefined,
): { end: string; start: string } | undefined {
  if (!value) {
    return undefined;
  }

  const parts = value.split(" to ");

  if (parts.length !== 2) {
    return undefined;
  }

  const start = parsePublishedDate(parts[0]?.trim() ?? "");
  const end = parsePublishedDate(parts[1]?.trim() ?? "");

  if (!start || !end || start.sortKey > end.sortKey) {
    return undefined;
  }

  return {
    end: end.inputValue,
    start: start.inputValue,
  };
}

export function isPublishedRangeValue(value: string | undefined) {
  return Boolean(getPublishedRangeInputValues(value));
}

export function isValidPublishedAtValue(value: string) {
  const candidate = value.trim();

  return (
    isPublishedPreset(candidate) ||
    /^\d+[smhdwMy]$/.test(candidate) ||
    isPublishedSpecialValue(candidate) ||
    Boolean(getPublishedRangeInputValues(candidate)) ||
    Boolean(parsePublishedDate(candidate))
  );
}

function publishedFilterValue(value: unknown): PublishedFilter | undefined {
  const candidate = stringValue(value);

  return candidate && isValidPublishedAtValue(candidate) ? candidate : undefined;
}

function formatPublishedDateInput(value: string) {
  const [year, month, day] = value.split("-").map(Number);

  return `${PUBLISHED_MONTHS[month - 1]} ${day}, ${year}`;
}

export function formatPublishedRangeValue(start: string, end: string) {
  const startDate = parsePublishedDate(start);
  const endDate = parsePublishedDate(end);

  if (!startDate || !endDate || startDate.sortKey > endDate.sortKey) {
    return undefined;
  }

  return `${formatPublishedDateInput(startDate.inputValue)} to ${formatPublishedDateInput(
    endDate.inputValue,
  )}`;
}

function finiteValue<T extends string>(values: readonly T[], value: string): value is T {
  return (values as readonly string[]).includes(value);
}

function isValidFacetValue(key: TorrentSearchFacetKey, value: string) {
  switch (key) {
    case "file_type":
      return finiteValue(FILE_TYPE_VALUES, value);
    case "language":
      return finiteValue(LANGUAGE_VALUES, value);
    case "video_resolution":
      return value === "null" || finiteValue(VIDEO_RESOLUTION_VALUES, value);
    case "video_source":
      return value === "null" || finiteValue(VIDEO_SOURCE_VALUES, value);
    case "genre":
    case "torrent_source":
    case "torrent_tag":
      return true;
  }
}

function normalizeFacetValues(key: TorrentSearchFacetKey, values: readonly string[]) {
  return Array.from(new Set(values.filter((value) => isValidFacetValue(key, value)))).sort();
}

export function isTorrentSearchFacetRelevant(
  key: TorrentSearchFacetKey,
  contentType: ContentTypeSelection | undefined,
) {
  const contentTypes = FACET_CONTENT_TYPES[key];

  return (
    !contentTypes ||
    Boolean(contentType && contentType !== "null" && contentTypes.includes(contentType))
  );
}

export function sanitizeFacetSelections(
  selections: TorrentSearchFacetSelections | undefined,
  contentType: ContentTypeSelection | undefined,
): TorrentSearchFacetSelections {
  const next: TorrentSearchFacetSelections = {};

  for (const key of TORRENT_SEARCH_FACET_KEYS) {
    if (!isTorrentSearchFacetRelevant(key, contentType)) {
      continue;
    }

    const values = normalizeFacetValues(key, selections?.[key] ?? []);

    if (values.length) {
      next[key] = values;
    }
  }

  return next;
}

function parseFacetSelections(params: SearchInput, contentType: ContentTypeSelection | undefined) {
  const selections: TorrentSearchFacetSelections = {};

  for (const key of TORRENT_SEARCH_FACET_KEYS) {
    const values = normalizeFacetValues(key, stringListValue(params[key]));

    if (values.length) {
      selections[key] = values;
    }
  }

  return sanitizeFacetSelections(selections, contentType);
}

function getDefaultTorrentOrderField(query: string): TorrentContentOrderByField {
  return query ? "relevance" : "published_at";
}

export function getDefaultOrderField(
  query: string,
  mode: SearchMode = "torrents",
): SearchOrderField {
  return mode === "files" ? "size" : getDefaultTorrentOrderField(query);
}

export function getDefaultDescending(field: SearchOrderField) {
  return (
    FILE_ORDER_OPTIONS.find((option) => option.field === field)?.defaultDescending ??
    ORDER_OPTIONS.find((option) => option.field === field)?.defaultDescending ??
    true
  );
}

export function isDefaultOrdering(search: TorrentSearchState) {
  return (
    search.order === getDefaultOrderField(search.query, search.mode) &&
    search.descending === getDefaultDescending(search.order)
  );
}

export function parseTorrentSearchParams(input: unknown): TorrentSearchState {
  const params = searchInput(input);
  const query = stringValue(params["query"]) ?? stringValue(params["q"]) ?? "";
  const mode = searchModeValue(params["mode"]);
  const contentType = contentTypeValue(params["content_type"]) ?? contentTypeValue(params["type"]);
  const requestedOrder = orderValue(params["order"], mode, query);
  const order = requestedOrder ?? getDefaultOrderField(query, mode);
  const requestedDescending = booleanValue(params["desc"]);

  return {
    contentType,
    descending: requestedDescending ?? getDefaultDescending(order),
    facets: parseFacetSelections(params, contentType),
    limit: integerValue(params["limit"], 1) ?? DEFAULT_SEARCH_LIMIT,
    maxSize: integerValue(params["max_size"], 1),
    maxSizeUnit: sizeUnitValue(params["max_size_unit"]),
    minSize: integerValue(params["min_size"], 1),
    minSizeUnit: sizeUnitValue(params["min_size_unit"]),
    mode,
    order,
    page: integerValue(params["page"], 1) ?? 1,
    publishedAt:
      publishedFilterValue(params["published_at"]) ?? publishedFilterValue(params["published"]),
    query,
  };
}

export function stringifyTorrentSearchParams(search: TorrentSearchState): TorrentSearchUrlParams {
  const next: TorrentSearchUrlParams = {};
  const facetSelections = sanitizeFacetSelections(search.facets, search.contentType);

  if (search.query) {
    next.query = search.query;
  }

  if (search.contentType) {
    next.content_type = search.contentType;
  }

  if (search.mode !== "torrents") {
    next.mode = search.mode;
  }

  if (search.page !== 1) {
    next.page = search.page;
  }

  if (search.limit !== DEFAULT_SEARCH_LIMIT) {
    next.limit = search.limit;
  }

  if (!isDefaultOrdering(search)) {
    next.order = search.order;
    next.desc = search.descending ? 1 : 0;
  }

  if (search.minSize) {
    next.min_size = search.minSize;
    next.min_size_unit = search.minSizeUnit;
  }

  if (search.maxSize) {
    next.max_size = search.maxSize;
    next.max_size_unit = search.maxSizeUnit;
  }

  if (search.publishedAt) {
    next.published_at = search.publishedAt;
  }

  for (const key of TORRENT_SEARCH_FACET_KEYS) {
    const values = facetSelections[key];

    if (values?.length) {
      next[key] = values.join(",");
    }
  }

  return next;
}

export function validateTorrentSearchParams(input: unknown): TorrentSearchUrlParams {
  return stringifyTorrentSearchParams(parseTorrentSearchParams(input));
}

export function normalizeLegacyTorrentSearch(input: unknown): LegacyTorrentSearchNormalization {
  const params = searchInput(input);

  if (!hasSearchParam(params, "torrent")) {
    return { kind: "none" };
  }

  const torrent = legacyTorrentValue(params);

  if (INFO_HASH_PATTERN.test(torrent)) {
    return {
      infoHash: torrent.toLowerCase(),
      kind: "detail",
    };
  }

  return {
    kind: "search",
    search: validateTorrentSearchParams(params),
  };
}

export const stripTorrentSearchDefaults: SearchMiddleware<TorrentSearchUrlParams> = ({
  next,
  search,
}) => stringifyTorrentSearchParams(parseTorrentSearchParams(next(search)));

export function sizeToBytes(size: number | undefined, unit: SizeUnit): number | undefined {
  if (!size) {
    return undefined;
  }

  switch (unit) {
    case "KB":
      return Math.floor(size * 1000);
    case "MB":
      return Math.floor(size * 1000 * 1000);
    case "GB":
      return Math.floor(size * 1000 * 1000) * 1000;
    case "TB":
      return Math.floor(size * 1000 * 1000) * 1000 * 1000;
    case "KiB":
      return Math.floor(size * 1024);
    case "MiB":
      return Math.floor(size * 1024 * 1024);
    case "GiB":
      return Math.floor(size * 1024 * 1024) * 1024;
    case "TiB":
      return Math.floor(size * 1024 * 1024) * 1024 * 1024;
  }
}

export function getTorrentSearchOrderBy(search: TorrentSearchState): TorrentContentOrderByInput[] {
  const field = isTorrentOrderField(search.order)
    ? search.order
    : getDefaultTorrentOrderField(search.query);

  return [
    {
      descending: search.descending,
      field,
    },
  ];
}

export function getFileSearchSort(search: TorrentSearchState): FileSearchSortInput[] {
  const field =
    isFileOrderField(search.order) && isFileOrderAllowedForQuery(search.order, search.query)
      ? search.order
      : "size";

  return [
    {
      descending: search.descending,
      field,
    },
  ];
}

export function getTorrentSearchFacets(
  search: TorrentSearchState,
  activeFacetKeys: readonly TorrentSearchFacetKey[] = [],
): TorrentContentFacetsInput {
  const min = sizeToBytes(search.minSize, search.minSizeUnit);
  const max = sizeToBytes(search.maxSize, search.maxSizeUnit);
  const activeFacets = new Set(activeFacetKeys);
  const facetSelections = sanitizeFacetSelections(search.facets, search.contentType);
  const selectedValues = (key: TorrentSearchFacetKey) => facetSelections[key] ?? [];
  const isActive = (key: TorrentSearchFacetKey) =>
    isTorrentSearchFacetRelevant(key, search.contentType) &&
    (activeFacets.has(key) || selectedValues(key).length > 0);
  const facets: TorrentContentFacetsInput = {
    contentType: {
      aggregate: true,
      filter: search.contentType
        ? [search.contentType === "null" ? null : search.contentType]
        : undefined,
    },
  };
  const torrentSource = selectedValues("torrent_source");
  const torrentTag = selectedValues("torrent_tag");
  const torrentFileType = selectedValues("file_type");
  const language = selectedValues("language");
  const genre = selectedValues("genre");
  const videoResolution = selectedValues("video_resolution");
  const videoSource = selectedValues("video_source");

  if (min || max) {
    facets.sizeRange = {
      max,
      min,
    };
  }

  if (search.publishedAt) {
    facets.publishedAt = search.publishedAt;
  }

  if (isActive("torrent_source")) {
    facets.torrentSource = {
      aggregate: true,
      filter: torrentSource.length ? torrentSource : undefined,
    };
  }

  if (isActive("torrent_tag")) {
    facets.torrentTag = {
      aggregate: true,
      filter: torrentTag.length ? torrentTag : undefined,
    };
  }

  if (isActive("file_type")) {
    facets.torrentFileType = {
      aggregate: true,
      filter: torrentFileType.length ? (torrentFileType as FileType[]) : undefined,
    };
  }

  if (isActive("language")) {
    facets.language = {
      aggregate: true,
      filter: language.length ? (language as Language[]) : undefined,
    };
  }

  if (isActive("genre")) {
    facets.genre = {
      aggregate: true,
      filter: genre.length ? genre : undefined,
    };
  }

  if (isActive("video_resolution")) {
    facets.videoResolution = {
      aggregate: true,
      filter: videoResolution.length
        ? videoResolution.map((value) => (value === "null" ? null : (value as VideoResolution)))
        : undefined,
    };
  }

  if (isActive("video_source")) {
    facets.videoSource = {
      aggregate: true,
      filter: videoSource.length
        ? videoSource.map((value) => (value === "null" ? null : (value as VideoSource)))
        : undefined,
    };
  }

  return facets;
}

export function updateQuery(search: TorrentSearchState, query: string): TorrentSearchState {
  const trimmedQuery = query.trim();
  const currentOrderIsDefault = isDefaultOrdering(search);
  const order =
    currentOrderIsDefault || !isOrderAllowedForMode(search.mode, search.order, trimmedQuery)
      ? getDefaultOrderField(trimmedQuery, search.mode)
      : search.order;

  return {
    ...search,
    descending: order === search.order ? search.descending : getDefaultDescending(order),
    order,
    page: 1,
    query: trimmedQuery,
  };
}

export function updateSearchMode(search: TorrentSearchState, mode: SearchMode): TorrentSearchState {
  const currentOrderIsDefault = isDefaultOrdering(search);
  const order =
    currentOrderIsDefault || !isOrderAllowedForMode(mode, search.order, search.query)
      ? getDefaultOrderField(search.query, mode)
      : search.order;

  return {
    ...search,
    descending: order === search.order ? search.descending : getDefaultDescending(order),
    mode,
    order,
    page: 1,
  };
}
