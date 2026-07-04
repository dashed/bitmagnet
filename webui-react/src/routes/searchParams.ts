import type { SearchMiddleware } from "@tanstack/react-router";

import type {
  ContentType,
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

export const SIZE_UNITS = ["KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB"] as const;
export type SizeUnit = (typeof SIZE_UNITS)[number];

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

export const PUBLISHED_PRESETS = [
  { labelKey: "search.publishedLastDay", value: "24h" },
  { labelKey: "search.publishedLastWeek", value: "7d" },
  { labelKey: "search.publishedLastMonth", value: "30d" },
  { labelKey: "search.publishedLastThreeMonths", value: "90d" },
  { labelKey: "search.publishedLastYear", value: "last year" },
] as const;

export type PublishedPreset = (typeof PUBLISHED_PRESETS)[number]["value"];

type SearchInput = Record<string, unknown>;

export type TorrentSearchUrlParams = {
  content_type?: ContentTypeSelection;
  desc?: 0 | 1;
  limit?: number;
  max_size?: number;
  max_size_unit?: SizeUnit;
  min_size?: number;
  min_size_unit?: SizeUnit;
  order?: TorrentContentOrderByField;
  page?: number;
  published_at?: PublishedPreset;
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
  order: TorrentContentOrderByField;
  page: number;
  publishedAt?: PublishedPreset;
  query: string;
};

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

    return decoded
      .split(",")
      .map((part) => safeDecode(part).trim())
      .filter(Boolean);
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

function orderValue(value: unknown): TorrentContentOrderByField | undefined {
  const candidate = stringValue(value);

  return ORDER_OPTIONS.some((option) => option.field === candidate)
    ? (candidate as TorrentContentOrderByField)
    : undefined;
}

function sizeUnitValue(value: unknown): SizeUnit {
  const candidate = stringValue(value);

  return SIZE_UNITS.includes(candidate as SizeUnit) ? (candidate as SizeUnit) : DEFAULT_SIZE_UNIT;
}

function publishedPresetValue(value: unknown): PublishedPreset | undefined {
  const candidate = stringValue(value);

  return PUBLISHED_PRESETS.some((preset) => preset.value === candidate)
    ? (candidate as PublishedPreset)
    : undefined;
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

export function getDefaultOrderField(query: string): TorrentContentOrderByField {
  return query ? "relevance" : "published_at";
}

export function getDefaultDescending(field: TorrentContentOrderByField) {
  return ORDER_OPTIONS.find((option) => option.field === field)?.defaultDescending ?? true;
}

export function isDefaultOrdering(search: TorrentSearchState) {
  return search.order === getDefaultOrderField(search.query) && search.descending;
}

export function parseTorrentSearchParams(input: unknown): TorrentSearchState {
  const params = (input && typeof input === "object" ? input : {}) as SearchInput;
  const query = stringValue(params["query"]) ?? stringValue(params["q"]) ?? "";
  const contentType = contentTypeValue(params["content_type"]) ?? contentTypeValue(params["type"]);
  const requestedOrder = orderValue(params["order"]);
  const order =
    requestedOrder && (requestedOrder !== "relevance" || query)
      ? requestedOrder
      : getDefaultOrderField(query);
  const requestedDescending = booleanValue(params["desc"]);

  return {
    contentType,
    descending: requestedDescending ?? (requestedOrder ? getDefaultDescending(order) : true),
    facets: parseFacetSelections(params, contentType),
    limit: integerValue(params["limit"], 1) ?? DEFAULT_SEARCH_LIMIT,
    maxSize: integerValue(params["max_size"], 1),
    maxSizeUnit: sizeUnitValue(params["max_size_unit"]),
    minSize: integerValue(params["min_size"], 1),
    minSizeUnit: sizeUnitValue(params["min_size_unit"]),
    order,
    page: integerValue(params["page"], 1) ?? 1,
    publishedAt:
      publishedPresetValue(params["published_at"]) ?? publishedPresetValue(params["published"]),
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
  return [
    {
      descending: search.descending,
      field: search.order,
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
    currentOrderIsDefault || (!trimmedQuery && search.order === "relevance")
      ? getDefaultOrderField(trimmedQuery)
      : search.order;

  return {
    ...search,
    descending: order === search.order ? search.descending : true,
    order,
    page: 1,
    query: trimmedQuery,
  };
}
