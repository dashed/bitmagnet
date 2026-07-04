/* eslint-disable */
import { DocumentTypeDecoration } from "@graphql-typed-document-node/core";
export type Maybe<T> = T | null;
export type InputMaybe<T> = T | null | undefined;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type MakeEmpty<T extends { [key: string]: unknown }, K extends keyof T> = {
  [_ in K]?: never;
};
export type Incremental<T> =
  | T
  | { [P in keyof T]?: P extends " $fragmentName" | "__typename" ? T[P] : never };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string };
  String: { input: string; output: string };
  Boolean: { input: boolean; output: boolean };
  Int: { input: number; output: number };
  Float: { input: number; output: number };
  Date: { input: string; output: string };
  DateTime: { input: string; output: string };
  Duration: { input: string; output: string };
  Hash20: { input: string; output: string };
  Hash32: { input: string; output: string };
  Void: { input: null | undefined; output: null | undefined };
  Year: { input: number; output: number };
};

export type Content = {
  __typename?: "Content";
  adult?: Maybe<Scalars["Boolean"]["output"]>;
  attributes: Array<ContentAttribute>;
  collections: Array<ContentCollection>;
  createdAt: Scalars["DateTime"]["output"];
  externalLinks: Array<ExternalLink>;
  id: Scalars["String"]["output"];
  metadataSource: MetadataSource;
  originalLanguage?: Maybe<LanguageInfo>;
  originalTitle?: Maybe<Scalars["String"]["output"]>;
  overview?: Maybe<Scalars["String"]["output"]>;
  popularity?: Maybe<Scalars["Float"]["output"]>;
  releaseDate?: Maybe<Scalars["Date"]["output"]>;
  releaseYear?: Maybe<Scalars["Year"]["output"]>;
  runtime?: Maybe<Scalars["Int"]["output"]>;
  source: Scalars["String"]["output"];
  title: Scalars["String"]["output"];
  type: ContentType;
  updatedAt: Scalars["DateTime"]["output"];
  voteAverage?: Maybe<Scalars["Float"]["output"]>;
  voteCount?: Maybe<Scalars["Int"]["output"]>;
};

export type ContentAttribute = {
  __typename?: "ContentAttribute";
  createdAt: Scalars["DateTime"]["output"];
  key: Scalars["String"]["output"];
  metadataSource: MetadataSource;
  source: Scalars["String"]["output"];
  updatedAt: Scalars["DateTime"]["output"];
  value: Scalars["String"]["output"];
};

export type ContentCollection = {
  __typename?: "ContentCollection";
  createdAt: Scalars["DateTime"]["output"];
  id: Scalars["String"]["output"];
  metadataSource: MetadataSource;
  name: Scalars["String"]["output"];
  source: Scalars["String"]["output"];
  type: Scalars["String"]["output"];
  updatedAt: Scalars["DateTime"]["output"];
};

export type ContentType =
  | "audiobook"
  | "comic"
  | "ebook"
  | "game"
  | "movie"
  | "music"
  | "software"
  | "tv_show"
  | "xxx";

export type ContentTypeAgg = {
  __typename?: "ContentTypeAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value?: Maybe<ContentType>;
};

export type ContentTypeFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<InputMaybe<ContentType>>>;
};

export type Episodes = {
  __typename?: "Episodes";
  label: Scalars["String"]["output"];
  seasons: Array<Season>;
};

export type ExternalLink = {
  __typename?: "ExternalLink";
  metadataSource: MetadataSource;
  url: Scalars["String"]["output"];
};

export type FacetLogic = "and" | "or";

export type FileSearchInput = {
  extensions?: InputMaybe<Array<Scalars["String"]["input"]>>;
  infoHash?: InputMaybe<Scalars["Hash20"]["input"]>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  maxSize?: InputMaybe<Scalars["Int"]["input"]>;
  minSize?: InputMaybe<Scalars["Int"]["input"]>;
  offset?: InputMaybe<Scalars["Int"]["input"]>;
  query?: InputMaybe<Scalars["String"]["input"]>;
  sort?: InputMaybe<Array<FileSearchSortInput>>;
  /**
   * When false, skip the exact L2 count RPC and return totalCount as 0.
   * Omitted defaults to true for compatibility with existing callers.
   */
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type FileSearchItem = {
  __typename?: "FileSearchItem";
  extension: Scalars["String"]["output"];
  index: Scalars["Int"]["output"];
  infoHash: Scalars["Hash20"]["output"];
  path: Scalars["String"]["output"];
  size: Scalars["Int"]["output"];
  torrentContent: TorrentContent;
};

export type FileSearchResult = {
  __typename?: "FileSearchResult";
  hasNextPage: Scalars["Boolean"]["output"];
  items: Array<FileSearchItem>;
  totalCount: Scalars["Int"]["output"];
};

export type FileSearchSortInput = {
  descending?: InputMaybe<Scalars["Boolean"]["input"]>;
  field: Scalars["String"]["input"];
};

export type FileType =
  | "archive"
  | "audio"
  | "data"
  | "document"
  | "image"
  | "software"
  | "subtitles"
  | "video";

export type FilesStatus = "multi" | "no_info" | "over_threshold" | "single";

export type GenreAgg = {
  __typename?: "GenreAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value: Scalars["String"]["output"];
};

export type GenreFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<Scalars["String"]["input"]>>;
  logic?: InputMaybe<FacetLogic>;
};

export type HealthCheck = {
  __typename?: "HealthCheck";
  error?: Maybe<Scalars["String"]["output"]>;
  key: Scalars["String"]["output"];
  status: HealthStatus;
  timestamp: Scalars["DateTime"]["output"];
};

export type HealthQuery = {
  __typename?: "HealthQuery";
  checks: Array<HealthCheck>;
  status: HealthStatus;
};

export type HealthStatus = "down" | "inactive" | "unknown" | "up";

export type Language =
  | "af"
  | "ar"
  | "az"
  | "be"
  | "bg"
  | "bs"
  | "ca"
  | "ce"
  | "co"
  | "cs"
  | "cy"
  | "da"
  | "de"
  | "el"
  | "en"
  | "es"
  | "et"
  | "eu"
  | "fa"
  | "fi"
  | "fr"
  | "he"
  | "hi"
  | "hr"
  | "hu"
  | "hy"
  | "id"
  | "is"
  | "it"
  | "ja"
  | "ka"
  | "ko"
  | "ku"
  | "lt"
  | "lv"
  | "mi"
  | "mk"
  | "ml"
  | "mn"
  | "ms"
  | "mt"
  | "nl"
  | "no"
  | "pl"
  | "pt"
  | "ro"
  | "ru"
  | "sa"
  | "sk"
  | "sl"
  | "sm"
  | "so"
  | "sr"
  | "sv"
  | "ta"
  | "th"
  | "tr"
  | "uk"
  | "vi"
  | "yi"
  | "zh"
  | "zu";

export type LanguageAgg = {
  __typename?: "LanguageAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value: Language;
};

export type LanguageFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<Language>>;
};

export type LanguageInfo = {
  __typename?: "LanguageInfo";
  id: Scalars["String"]["output"];
  name: Scalars["String"]["output"];
};

export type MetadataSource = {
  __typename?: "MetadataSource";
  key: Scalars["String"]["output"];
  name: Scalars["String"]["output"];
};

export type MetricsBucketDuration = "day" | "hour" | "minute";

export type Mutation = {
  __typename?: "Mutation";
  queue: QueueMutation;
  torrent: TorrentMutation;
};

export type PathTypeaheadInput = {
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  prefix: Scalars["String"]["input"];
};

export type PathTypeaheadResult = {
  __typename?: "PathTypeaheadResult";
  suggestions: Array<Scalars["String"]["output"]>;
};

export type Query = {
  __typename?: "Query";
  health: HealthQuery;
  queue: QueueQuery;
  torrent: TorrentQuery;
  torrentContent: TorrentContentQuery;
  version: Scalars["String"]["output"];
  workers: WorkersQuery;
};

export type QueueEnqueueReprocessTorrentsBatchInput = {
  apisDisabled?: InputMaybe<Scalars["Boolean"]["input"]>;
  batchSize?: InputMaybe<Scalars["Int"]["input"]>;
  chunkSize?: InputMaybe<Scalars["Int"]["input"]>;
  classifierRematch?: InputMaybe<Scalars["Boolean"]["input"]>;
  classifierWorkflow?: InputMaybe<Scalars["String"]["input"]>;
  contentTypes?: InputMaybe<Array<InputMaybe<ContentType>>>;
  localSearchDisabled?: InputMaybe<Scalars["Boolean"]["input"]>;
  orphans?: InputMaybe<Scalars["Boolean"]["input"]>;
  purge?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type QueueJob = {
  __typename?: "QueueJob";
  createdAt: Scalars["DateTime"]["output"];
  error?: Maybe<Scalars["String"]["output"]>;
  id: Scalars["ID"]["output"];
  maxRetries: Scalars["Int"]["output"];
  payload: Scalars["String"]["output"];
  priority: Scalars["Int"]["output"];
  queue: Scalars["String"]["output"];
  ranAt?: Maybe<Scalars["DateTime"]["output"]>;
  retries: Scalars["Int"]["output"];
  runAfter: Scalars["DateTime"]["output"];
  status: QueueJobStatus;
};

export type QueueJobQueueAgg = {
  __typename?: "QueueJobQueueAgg";
  count: Scalars["Int"]["output"];
  label: Scalars["String"]["output"];
  value: Scalars["String"]["output"];
};

export type QueueJobQueueFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<Scalars["String"]["input"]>>;
};

export type QueueJobStatus = "failed" | "pending" | "processed" | "retry";

export type QueueJobStatusAgg = {
  __typename?: "QueueJobStatusAgg";
  count: Scalars["Int"]["output"];
  label: Scalars["String"]["output"];
  value: QueueJobStatus;
};

export type QueueJobStatusFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<QueueJobStatus>>;
};

export type QueueJobsAggregations = {
  __typename?: "QueueJobsAggregations";
  queue?: Maybe<Array<QueueJobQueueAgg>>;
  status?: Maybe<Array<QueueJobStatusAgg>>;
};

export type QueueJobsFacetsInput = {
  queue?: InputMaybe<QueueJobQueueFacetInput>;
  status?: InputMaybe<QueueJobStatusFacetInput>;
};

export type QueueJobsOrderByField = "created_at" | "priority" | "ran_at";

export type QueueJobsOrderByInput = {
  descending?: InputMaybe<Scalars["Boolean"]["input"]>;
  field: QueueJobsOrderByField;
};

export type QueueJobsQueryInput = {
  facets?: InputMaybe<QueueJobsFacetsInput>;
  hasNextPage?: InputMaybe<Scalars["Boolean"]["input"]>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  offset?: InputMaybe<Scalars["Int"]["input"]>;
  orderBy?: InputMaybe<Array<QueueJobsOrderByInput>>;
  page?: InputMaybe<Scalars["Int"]["input"]>;
  queues?: InputMaybe<Array<Scalars["String"]["input"]>>;
  statuses?: InputMaybe<Array<QueueJobStatus>>;
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type QueueJobsQueryResult = {
  __typename?: "QueueJobsQueryResult";
  aggregations: QueueJobsAggregations;
  hasNextPage?: Maybe<Scalars["Boolean"]["output"]>;
  items: Array<QueueJob>;
  totalCount: Scalars["Int"]["output"];
};

export type QueueMetricsBucket = {
  __typename?: "QueueMetricsBucket";
  count: Scalars["Int"]["output"];
  createdAtBucket: Scalars["DateTime"]["output"];
  latency?: Maybe<Scalars["Duration"]["output"]>;
  queue: Scalars["String"]["output"];
  ranAtBucket?: Maybe<Scalars["DateTime"]["output"]>;
  status: QueueJobStatus;
};

export type QueueMetricsQueryInput = {
  bucketDuration: MetricsBucketDuration;
  endTime?: InputMaybe<Scalars["DateTime"]["input"]>;
  queues?: InputMaybe<Array<Scalars["String"]["input"]>>;
  startTime?: InputMaybe<Scalars["DateTime"]["input"]>;
  statuses?: InputMaybe<Array<QueueJobStatus>>;
};

export type QueueMetricsQueryResult = {
  __typename?: "QueueMetricsQueryResult";
  buckets: Array<QueueMetricsBucket>;
};

export type QueueMutation = {
  __typename?: "QueueMutation";
  enqueueReprocessTorrentsBatch?: Maybe<Scalars["Void"]["output"]>;
  purgeJobs?: Maybe<Scalars["Void"]["output"]>;
};

export type QueueMutationEnqueueReprocessTorrentsBatchArgs = {
  input?: InputMaybe<QueueEnqueueReprocessTorrentsBatchInput>;
};

export type QueueMutationPurgeJobsArgs = {
  input: QueuePurgeJobsInput;
};

export type QueuePurgeJobsInput = {
  queues?: InputMaybe<Array<Scalars["String"]["input"]>>;
  statuses?: InputMaybe<Array<QueueJobStatus>>;
};

export type QueueQuery = {
  __typename?: "QueueQuery";
  jobs: QueueJobsQueryResult;
  metrics: QueueMetricsQueryResult;
};

export type QueueQueryJobsArgs = {
  input: QueueJobsQueryInput;
};

export type QueueQueryMetricsArgs = {
  input: QueueMetricsQueryInput;
};

export type ReleaseYearAgg = {
  __typename?: "ReleaseYearAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value?: Maybe<Scalars["Year"]["output"]>;
};

export type ReleaseYearFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<InputMaybe<Scalars["Year"]["input"]>>>;
};

export type Season = {
  __typename?: "Season";
  episodes?: Maybe<Array<Scalars["Int"]["output"]>>;
  season: Scalars["Int"]["output"];
};

export type SizeRangeInput = {
  max?: InputMaybe<Scalars["Int"]["input"]>;
  min?: InputMaybe<Scalars["Int"]["input"]>;
};

export type SuggestTagsQueryInput = {
  exclusions?: InputMaybe<Array<Scalars["String"]["input"]>>;
  prefix?: InputMaybe<Scalars["String"]["input"]>;
};

export type SuggestedTag = {
  __typename?: "SuggestedTag";
  count: Scalars["Int"]["output"];
  name: Scalars["String"]["output"];
};

export type Torrent = {
  __typename?: "Torrent";
  createdAt: Scalars["DateTime"]["output"];
  extension?: Maybe<Scalars["String"]["output"]>;
  fileType?: Maybe<FileType>;
  fileTypes?: Maybe<Array<FileType>>;
  files?: Maybe<Array<TorrentFile>>;
  filesCount?: Maybe<Scalars["Int"]["output"]>;
  filesStatus: FilesStatus;
  hasFilesInfo: Scalars["Boolean"]["output"];
  infoHash: Scalars["Hash20"]["output"];
  infoHashV2?: Maybe<Scalars["Hash32"]["output"]>;
  leechers?: Maybe<Scalars["Int"]["output"]>;
  magnetUri: Scalars["String"]["output"];
  metaVersion?: Maybe<Scalars["Int"]["output"]>;
  name: Scalars["String"]["output"];
  seeders?: Maybe<Scalars["Int"]["output"]>;
  singleFile?: Maybe<Scalars["Boolean"]["output"]>;
  size: Scalars["Int"]["output"];
  sources: Array<TorrentSourceInfo>;
  tagNames: Array<Scalars["String"]["output"]>;
  updatedAt: Scalars["DateTime"]["output"];
};

export type TorrentContent = {
  __typename?: "TorrentContent";
  content?: Maybe<Content>;
  contentId?: Maybe<Scalars["String"]["output"]>;
  contentSource?: Maybe<Scalars["String"]["output"]>;
  contentType?: Maybe<ContentType>;
  createdAt: Scalars["DateTime"]["output"];
  dhtFirstSeenAt?: Maybe<Scalars["DateTime"]["output"]>;
  dhtLastSeenAt?: Maybe<Scalars["DateTime"]["output"]>;
  dhtSeenCount: Scalars["Int"]["output"];
  episodes?: Maybe<Episodes>;
  id: Scalars["ID"]["output"];
  infoHash: Scalars["Hash20"]["output"];
  languages?: Maybe<Array<LanguageInfo>>;
  leechers?: Maybe<Scalars["Int"]["output"]>;
  publishedAt: Scalars["DateTime"]["output"];
  releaseGroup?: Maybe<Scalars["String"]["output"]>;
  seeders?: Maybe<Scalars["Int"]["output"]>;
  title: Scalars["String"]["output"];
  torrent: Torrent;
  updatedAt: Scalars["DateTime"]["output"];
  video3d?: Maybe<Video3D>;
  videoCodec?: Maybe<VideoCodec>;
  videoModifier?: Maybe<VideoModifier>;
  videoResolution?: Maybe<VideoResolution>;
  videoSource?: Maybe<VideoSource>;
};

export type TorrentContentAggregations = {
  __typename?: "TorrentContentAggregations";
  contentType?: Maybe<Array<ContentTypeAgg>>;
  genre?: Maybe<Array<GenreAgg>>;
  language?: Maybe<Array<LanguageAgg>>;
  releaseYear?: Maybe<Array<ReleaseYearAgg>>;
  torrentFileType?: Maybe<Array<TorrentFileTypeAgg>>;
  torrentSource?: Maybe<Array<TorrentSourceAgg>>;
  torrentTag?: Maybe<Array<TorrentTagAgg>>;
  videoResolution?: Maybe<Array<VideoResolutionAgg>>;
  videoSource?: Maybe<Array<VideoSourceAgg>>;
};

/**
 * TorrentContentCollapsePathsInput drives the collapse:path query: the L3-routed
 * grouping of matching torrents by their distinct matched file path. It carries the
 * raw path free-text only (no facets in v1).
 */
export type TorrentContentCollapsePathsInput = {
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  offset?: InputMaybe<Scalars["Int"]["input"]>;
  queryString: Scalars["String"]["input"];
};

export type TorrentContentCollapsePathsResult = {
  __typename?: "TorrentContentCollapsePathsResult";
  groups: Array<TorrentContentPathGroup>;
};

export type TorrentContentFacetsInput = {
  contentType?: InputMaybe<ContentTypeFacetInput>;
  genre?: InputMaybe<GenreFacetInput>;
  language?: InputMaybe<LanguageFacetInput>;
  publishedAt?: InputMaybe<Scalars["String"]["input"]>;
  releaseYear?: InputMaybe<ReleaseYearFacetInput>;
  sizeRange?: InputMaybe<SizeRangeInput>;
  torrentFileType?: InputMaybe<TorrentFileTypeFacetInput>;
  torrentSource?: InputMaybe<TorrentSourceFacetInput>;
  torrentTag?: InputMaybe<TorrentTagFacetInput>;
  videoResolution?: InputMaybe<VideoResolutionFacetInput>;
  videoSource?: InputMaybe<VideoSourceFacetInput>;
};

export type TorrentContentOrderByField =
  | "files_count"
  | "info_hash"
  | "leechers"
  | "name"
  | "published_at"
  | "relevance"
  | "seeders"
  | "size"
  | "updated_at";

export type TorrentContentOrderByInput = {
  descending?: InputMaybe<Scalars["Boolean"]["input"]>;
  field: TorrentContentOrderByField;
};

/**
 * TorrentContentPathGroup is one distinct matched file path and the info hashes of
 * the candidate torrents that contain a file at that path. v1 is UNHYDRATED: it
 * returns raw info hashes only; clients hydrate via the existing search-by-infoHashes.
 */
export type TorrentContentPathGroup = {
  __typename?: "TorrentContentPathGroup";
  infoHashes: Array<Scalars["Hash20"]["output"]>;
  path: Scalars["String"]["output"];
};

export type TorrentContentQuery = {
  __typename?: "TorrentContentQuery";
  collapsePaths: TorrentContentCollapsePathsResult;
  fileSearch: FileSearchResult;
  pathTypeahead: PathTypeaheadResult;
  search: TorrentContentSearchResult;
};

export type TorrentContentQueryCollapsePathsArgs = {
  input: TorrentContentCollapsePathsInput;
};

export type TorrentContentQueryFileSearchArgs = {
  input: FileSearchInput;
};

export type TorrentContentQueryPathTypeaheadArgs = {
  input: PathTypeaheadInput;
};

export type TorrentContentQuerySearchArgs = {
  input: TorrentContentSearchQueryInput;
};

export type TorrentContentSearchQueryInput = {
  aggregationBudget?: InputMaybe<Scalars["Float"]["input"]>;
  cached?: InputMaybe<Scalars["Boolean"]["input"]>;
  facets?: InputMaybe<TorrentContentFacetsInput>;
  /** hasNextPage if true, the search result will include the hasNextPage field, indicating if there are more results to fetch */
  hasNextPage?: InputMaybe<Scalars["Boolean"]["input"]>;
  infoHashes?: InputMaybe<Array<Scalars["Hash20"]["input"]>>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  offset?: InputMaybe<Scalars["Int"]["input"]>;
  orderBy?: InputMaybe<Array<TorrentContentOrderByInput>>;
  page?: InputMaybe<Scalars["Int"]["input"]>;
  queryString?: InputMaybe<Scalars["String"]["input"]>;
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type TorrentContentSearchResult = {
  __typename?: "TorrentContentSearchResult";
  aggregations: TorrentContentAggregations;
  /** hasNextPage is true if there are more results to fetch */
  hasNextPage?: Maybe<Scalars["Boolean"]["output"]>;
  items: Array<TorrentContent>;
  totalCount: Scalars["Int"]["output"];
  totalCountIsEstimate: Scalars["Boolean"]["output"];
};

export type TorrentFile = {
  __typename?: "TorrentFile";
  createdAt: Scalars["DateTime"]["output"];
  extension?: Maybe<Scalars["String"]["output"]>;
  fileType?: Maybe<FileType>;
  index: Scalars["Int"]["output"];
  infoHash: Scalars["Hash20"]["output"];
  path: Scalars["String"]["output"];
  size: Scalars["Int"]["output"];
  updatedAt: Scalars["DateTime"]["output"];
};

export type TorrentFileTypeAgg = {
  __typename?: "TorrentFileTypeAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value: FileType;
};

export type TorrentFileTypeFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<FileType>>;
  logic?: InputMaybe<FacetLogic>;
};

export type TorrentFilesOrderByField = "extension" | "index" | "path" | "size";

export type TorrentFilesOrderByInput = {
  descending?: InputMaybe<Scalars["Boolean"]["input"]>;
  field: TorrentFilesOrderByField;
};

export type TorrentFilesQueryInput = {
  cached?: InputMaybe<Scalars["Boolean"]["input"]>;
  hasNextPage?: InputMaybe<Scalars["Boolean"]["input"]>;
  infoHashes?: InputMaybe<Array<Scalars["Hash20"]["input"]>>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  offset?: InputMaybe<Scalars["Int"]["input"]>;
  orderBy?: InputMaybe<Array<TorrentFilesOrderByInput>>;
  page?: InputMaybe<Scalars["Int"]["input"]>;
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type TorrentFilesQueryResult = {
  __typename?: "TorrentFilesQueryResult";
  hasNextPage?: Maybe<Scalars["Boolean"]["output"]>;
  items: Array<TorrentFile>;
  totalCount: Scalars["Int"]["output"];
};

export type TorrentListSourcesResult = {
  __typename?: "TorrentListSourcesResult";
  sources: Array<TorrentSource>;
};

export type TorrentMetricsBucket = {
  __typename?: "TorrentMetricsBucket";
  bucket: Scalars["DateTime"]["output"];
  count: Scalars["Int"]["output"];
  source: Scalars["String"]["output"];
  updated: Scalars["Boolean"]["output"];
};

export type TorrentMetricsQueryInput = {
  bucketDuration: MetricsBucketDuration;
  endTime?: InputMaybe<Scalars["DateTime"]["input"]>;
  sources?: InputMaybe<Array<Scalars["String"]["input"]>>;
  startTime?: InputMaybe<Scalars["DateTime"]["input"]>;
};

export type TorrentMetricsQueryResult = {
  __typename?: "TorrentMetricsQueryResult";
  buckets: Array<TorrentMetricsBucket>;
};

export type TorrentMutation = {
  __typename?: "TorrentMutation";
  delete?: Maybe<Scalars["Void"]["output"]>;
  deleteTags?: Maybe<Scalars["Void"]["output"]>;
  putTags?: Maybe<Scalars["Void"]["output"]>;
  reprocess?: Maybe<Scalars["Void"]["output"]>;
  setTags?: Maybe<Scalars["Void"]["output"]>;
};

export type TorrentMutationDeleteArgs = {
  infoHashes: Array<Scalars["Hash20"]["input"]>;
};

export type TorrentMutationDeleteTagsArgs = {
  infoHashes?: InputMaybe<Array<Scalars["Hash20"]["input"]>>;
  tagNames?: InputMaybe<Array<Scalars["String"]["input"]>>;
};

export type TorrentMutationPutTagsArgs = {
  infoHashes: Array<Scalars["Hash20"]["input"]>;
  tagNames: Array<Scalars["String"]["input"]>;
};

export type TorrentMutationReprocessArgs = {
  input: TorrentReprocessInput;
};

export type TorrentMutationSetTagsArgs = {
  infoHashes: Array<Scalars["Hash20"]["input"]>;
  tagNames: Array<Scalars["String"]["input"]>;
};

export type TorrentQuery = {
  __typename?: "TorrentQuery";
  files: TorrentFilesQueryResult;
  listSources: TorrentListSourcesResult;
  metrics: TorrentMetricsQueryResult;
  suggestTags: TorrentSuggestTagsResult;
};

export type TorrentQueryFilesArgs = {
  input: TorrentFilesQueryInput;
};

export type TorrentQueryMetricsArgs = {
  input: TorrentMetricsQueryInput;
};

export type TorrentQuerySuggestTagsArgs = {
  input?: InputMaybe<SuggestTagsQueryInput>;
};

export type TorrentReprocessInput = {
  apisDisabled?: InputMaybe<Scalars["Boolean"]["input"]>;
  classifierRematch?: InputMaybe<Scalars["Boolean"]["input"]>;
  classifierWorkflow?: InputMaybe<Scalars["String"]["input"]>;
  infoHashes: Array<Scalars["Hash20"]["input"]>;
  localSearchDisabled?: InputMaybe<Scalars["Boolean"]["input"]>;
};

export type TorrentSource = {
  __typename?: "TorrentSource";
  key: Scalars["String"]["output"];
  name: Scalars["String"]["output"];
};

export type TorrentSourceAgg = {
  __typename?: "TorrentSourceAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value: Scalars["String"]["output"];
};

export type TorrentSourceFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<Scalars["String"]["input"]>>;
  logic?: InputMaybe<FacetLogic>;
};

export type TorrentSourceInfo = {
  __typename?: "TorrentSourceInfo";
  firstSeenAt: Scalars["DateTime"]["output"];
  importId?: Maybe<Scalars["String"]["output"]>;
  key: Scalars["String"]["output"];
  lastSeenAt: Scalars["DateTime"]["output"];
  leechers?: Maybe<Scalars["Int"]["output"]>;
  name: Scalars["String"]["output"];
  seeders?: Maybe<Scalars["Int"]["output"]>;
  seenCount: Scalars["Int"]["output"];
};

export type TorrentSuggestTagsResult = {
  __typename?: "TorrentSuggestTagsResult";
  suggestions: Array<SuggestedTag>;
};

export type TorrentTagAgg = {
  __typename?: "TorrentTagAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value: Scalars["String"]["output"];
};

export type TorrentTagFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<Scalars["String"]["input"]>>;
  logic?: InputMaybe<FacetLogic>;
};

export type Video3D = "V3D" | "V3DOU" | "V3DSBS";

export type VideoCodec = "DivX" | "H264" | "MPEG2" | "MPEG4" | "XviD" | "x264" | "x265";

export type VideoModifier = "BRDISK" | "RAWHD" | "REGIONAL" | "REMUX" | "SCREENER";

export type VideoResolution =
  | "V360p"
  | "V480p"
  | "V540p"
  | "V576p"
  | "V720p"
  | "V1080p"
  | "V1440p"
  | "V2160p"
  | "V4320p";

export type VideoResolutionAgg = {
  __typename?: "VideoResolutionAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value?: Maybe<VideoResolution>;
};

export type VideoResolutionFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<InputMaybe<VideoResolution>>>;
};

export type VideoSource =
  | "BluRay"
  | "CAM"
  | "DVD"
  | "TELECINE"
  | "TELESYNC"
  | "TV"
  | "WEBDL"
  | "WEBRip"
  | "WORKPRINT";

export type VideoSourceAgg = {
  __typename?: "VideoSourceAgg";
  count: Scalars["Int"]["output"];
  isEstimate: Scalars["Boolean"]["output"];
  label: Scalars["String"]["output"];
  value?: Maybe<VideoSource>;
};

export type VideoSourceFacetInput = {
  aggregate?: InputMaybe<Scalars["Boolean"]["input"]>;
  filter?: InputMaybe<Array<InputMaybe<VideoSource>>>;
};

export type Worker = {
  __typename?: "Worker";
  key: Scalars["String"]["output"];
  started: Scalars["Boolean"]["output"];
};

export type WorkersListAllQueryResult = {
  __typename?: "WorkersListAllQueryResult";
  workers: Array<Worker>;
};

export type WorkersQuery = {
  __typename?: "WorkersQuery";
  listAll: WorkersListAllQueryResult;
};

export type CollapsePathsQueryVariables = Exact<{
  input: TorrentContentCollapsePathsInput;
}>;

export type CollapsePathsQuery = {
  __typename?: "Query";
  torrentContent: {
    __typename?: "TorrentContentQuery";
    collapsePaths: {
      __typename?: "TorrentContentCollapsePathsResult";
      groups: Array<{
        __typename?: "TorrentContentPathGroup";
        path: string;
        infoHashes: Array<string>;
      }>;
    };
  };
};

export type FileSearchQueryVariables = Exact<{
  input: FileSearchInput;
}>;

export type FileSearchQuery = {
  __typename?: "Query";
  torrentContent: {
    __typename?: "TorrentContentQuery";
    fileSearch: {
      __typename?: "FileSearchResult";
      totalCount: number;
      hasNextPage: boolean;
      items: Array<{
        __typename?: "FileSearchItem";
        infoHash: string;
        index: number;
        path: string;
        extension: string;
        size: number;
        torrentContent: {
          __typename?: "TorrentContent";
          infoHash: string;
          seeders?: number | null;
          leechers?: number | null;
          publishedAt: string;
          updatedAt: string;
          dhtLastSeenAt?: string | null;
          dhtSeenCount: number;
          title: string;
          torrent: { __typename?: "Torrent"; name: string; magnetUri: string };
        };
      }>;
    };
  };
};

export type HealthCheckQueryVariables = Exact<{ [key: string]: never }>;

export type HealthCheckQuery = {
  __typename?: "Query";
  health: {
    __typename?: "HealthQuery";
    status: HealthStatus;
    checks: Array<{
      __typename?: "HealthCheck";
      key: string;
      status: HealthStatus;
      timestamp: string;
      error?: string | null;
    }>;
  };
  workers: {
    __typename?: "WorkersQuery";
    listAll: {
      __typename?: "WorkersListAllQueryResult";
      workers: Array<{ __typename?: "Worker"; key: string; started: boolean }>;
    };
  };
};

export type PathTypeaheadQueryVariables = Exact<{
  input: PathTypeaheadInput;
}>;

export type PathTypeaheadQuery = {
  __typename?: "Query";
  torrentContent: {
    __typename?: "TorrentContentQuery";
    pathTypeahead: { __typename?: "PathTypeaheadResult"; suggestions: Array<string> };
  };
};

export type QueueEnqueueReprocessTorrentsBatchMutationVariables = Exact<{
  input: QueueEnqueueReprocessTorrentsBatchInput;
}>;

export type QueueEnqueueReprocessTorrentsBatchMutation = {
  __typename?: "Mutation";
  queue: { __typename?: "QueueMutation"; enqueueReprocessTorrentsBatch?: null | undefined | null };
};

export type QueueJobsQueryVariables = Exact<{
  input: QueueJobsQueryInput;
}>;

export type QueueJobsQuery = {
  __typename?: "Query";
  queue: {
    __typename?: "QueueQuery";
    jobs: {
      __typename?: "QueueJobsQueryResult";
      totalCount: number;
      hasNextPage?: boolean | null;
      items: Array<{
        __typename?: "QueueJob";
        id: string;
        queue: string;
        status: QueueJobStatus;
        payload: string;
        priority: number;
        retries: number;
        maxRetries: number;
        runAfter: string;
        ranAt?: string | null;
        error?: string | null;
        createdAt: string;
      }>;
      aggregations: {
        __typename?: "QueueJobsAggregations";
        queue?: Array<{
          __typename?: "QueueJobQueueAgg";
          value: string;
          label: string;
          count: number;
        }> | null;
        status?: Array<{
          __typename?: "QueueJobStatusAgg";
          value: QueueJobStatus;
          label: string;
          count: number;
        }> | null;
      };
    };
  };
};

export type QueueMetricsQueryVariables = Exact<{
  input: QueueMetricsQueryInput;
}>;

export type QueueMetricsQuery = {
  __typename?: "Query";
  queue: {
    __typename?: "QueueQuery";
    metrics: {
      __typename?: "QueueMetricsQueryResult";
      buckets: Array<{
        __typename?: "QueueMetricsBucket";
        queue: string;
        status: QueueJobStatus;
        createdAtBucket: string;
        ranAtBucket?: string | null;
        count: number;
        latency?: string | null;
      }>;
    };
  };
};

export type QueuePurgeJobsMutationVariables = Exact<{
  input: QueuePurgeJobsInput;
}>;

export type QueuePurgeJobsMutation = {
  __typename?: "Mutation";
  queue: { __typename?: "QueueMutation"; purgeJobs?: null | undefined | null };
};

export type TorrentContentSearchQueryVariables = Exact<{
  cached?: InputMaybe<Scalars["Boolean"]["input"]>;
  facets?: InputMaybe<TorrentContentFacetsInput>;
  hasNextPage?: InputMaybe<Scalars["Boolean"]["input"]>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  orderBy?: InputMaybe<Array<TorrentContentOrderByInput> | TorrentContentOrderByInput>;
  page?: InputMaybe<Scalars["Int"]["input"]>;
  queryString?: InputMaybe<Scalars["String"]["input"]>;
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
}>;

export type TorrentContentSearchQuery = {
  __typename?: "Query";
  torrentContent: {
    __typename?: "TorrentContentQuery";
    search: {
      __typename?: "TorrentContentSearchResult";
      totalCount: number;
      totalCountIsEstimate: boolean;
      hasNextPage?: boolean | null;
      items: Array<{
        __typename?: "TorrentContent";
        infoHash: string;
        title: string;
        contentType?: ContentType | null;
        seeders?: number | null;
        leechers?: number | null;
        dhtSeenCount: number;
        dhtFirstSeenAt?: string | null;
        dhtLastSeenAt?: string | null;
        publishedAt: string;
        torrent: {
          __typename?: "Torrent";
          filesCount?: number | null;
          magnetUri: string;
          name: string;
          size: number;
        };
      }>;
      aggregations: {
        __typename?: "TorrentContentAggregations";
        contentType?: Array<{
          __typename?: "ContentTypeAgg";
          value?: ContentType | null;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        torrentSource?: Array<{
          __typename?: "TorrentSourceAgg";
          value: string;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        torrentTag?: Array<{
          __typename?: "TorrentTagAgg";
          value: string;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        torrentFileType?: Array<{
          __typename?: "TorrentFileTypeAgg";
          value: FileType;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        language?: Array<{
          __typename?: "LanguageAgg";
          value: Language;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        genre?: Array<{
          __typename?: "GenreAgg";
          value: string;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        videoResolution?: Array<{
          __typename?: "VideoResolutionAgg";
          value?: VideoResolution | null;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
        videoSource?: Array<{
          __typename?: "VideoSourceAgg";
          value?: VideoSource | null;
          label: string;
          count: number;
          isEstimate: boolean;
        }> | null;
      };
    };
  };
};

export type TorrentDeleteMutationVariables = Exact<{
  infoHashes: Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"];
}>;

export type TorrentDeleteMutation = {
  __typename?: "Mutation";
  torrent: { __typename?: "TorrentMutation"; delete?: null | undefined | null };
};

export type TorrentDeleteTagsMutationVariables = Exact<{
  infoHashes?: InputMaybe<Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"]>;
  tagNames?: InputMaybe<Array<Scalars["String"]["input"]> | Scalars["String"]["input"]>;
}>;

export type TorrentDeleteTagsMutation = {
  __typename?: "Mutation";
  torrent: { __typename?: "TorrentMutation"; deleteTags?: null | undefined | null };
};

export type TorrentDetailQueryVariables = Exact<{
  infoHashes: Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"];
  limit?: InputMaybe<Scalars["Int"]["input"]>;
}>;

export type TorrentDetailQuery = {
  __typename?: "Query";
  torrentContent: {
    __typename?: "TorrentContentQuery";
    search: {
      __typename?: "TorrentContentSearchResult";
      items: Array<{
        __typename?: "TorrentContent";
        infoHash: string;
        title: string;
        contentType?: ContentType | null;
        seeders?: number | null;
        leechers?: number | null;
        dhtSeenCount: number;
        dhtFirstSeenAt?: string | null;
        dhtLastSeenAt?: string | null;
        publishedAt: string;
        languages?: Array<{ __typename?: "LanguageInfo"; id: string; name: string }> | null;
        episodes?: { __typename?: "Episodes"; label: string } | null;
        torrent: {
          __typename?: "Torrent";
          name: string;
          size: number;
          filesStatus: FilesStatus;
          filesCount?: number | null;
          fileType?: FileType | null;
          magnetUri: string;
          sources: Array<{
            __typename?: "TorrentSourceInfo";
            key: string;
            name: string;
            seenCount: number;
            firstSeenAt: string;
            lastSeenAt: string;
          }>;
        };
        content?: {
          __typename?: "Content";
          title: string;
          originalTitle?: string | null;
          releaseDate?: string | null;
          releaseYear?: number | null;
          overview?: string | null;
          voteAverage?: number | null;
          voteCount?: number | null;
          originalLanguage?: { __typename?: "LanguageInfo"; id: string; name: string } | null;
          attributes: Array<{
            __typename?: "ContentAttribute";
            source: string;
            key: string;
            value: string;
          }>;
          collections: Array<{ __typename?: "ContentCollection"; type: string; name: string }>;
          externalLinks: Array<{
            __typename?: "ExternalLink";
            url: string;
            metadataSource: { __typename?: "MetadataSource"; key: string; name: string };
          }>;
        } | null;
      }>;
    };
  };
};

export type TorrentFilesQueryVariables = Exact<{
  hasNextPage?: InputMaybe<Scalars["Boolean"]["input"]>;
  infoHashes?: InputMaybe<Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"]>;
  limit?: InputMaybe<Scalars["Int"]["input"]>;
  orderBy?: InputMaybe<Array<TorrentFilesOrderByInput> | TorrentFilesOrderByInput>;
  totalCount?: InputMaybe<Scalars["Boolean"]["input"]>;
}>;

export type TorrentFilesQuery = {
  __typename?: "Query";
  torrent: {
    __typename?: "TorrentQuery";
    files: {
      __typename?: "TorrentFilesQueryResult";
      totalCount: number;
      hasNextPage?: boolean | null;
      items: Array<{
        __typename?: "TorrentFile";
        index: number;
        path: string;
        fileType?: FileType | null;
        size: number;
      }>;
    };
  };
};

export type TorrentMetricsQueryVariables = Exact<{
  input: TorrentMetricsQueryInput;
}>;

export type TorrentMetricsQuery = {
  __typename?: "Query";
  torrent: {
    __typename?: "TorrentQuery";
    metrics: {
      __typename?: "TorrentMetricsQueryResult";
      buckets: Array<{
        __typename?: "TorrentMetricsBucket";
        source: string;
        updated: boolean;
        bucket: string;
        count: number;
      }>;
    };
    listSources: {
      __typename?: "TorrentListSourcesResult";
      sources: Array<{ __typename?: "TorrentSource"; key: string; name: string }>;
    };
  };
};

export type TorrentPutTagsMutationVariables = Exact<{
  infoHashes: Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"];
  tagNames: Array<Scalars["String"]["input"]> | Scalars["String"]["input"];
}>;

export type TorrentPutTagsMutation = {
  __typename?: "Mutation";
  torrent: { __typename?: "TorrentMutation"; putTags?: null | undefined | null };
};

export type TorrentReprocessMutationVariables = Exact<{
  input: TorrentReprocessInput;
}>;

export type TorrentReprocessMutation = {
  __typename?: "Mutation";
  torrent: { __typename?: "TorrentMutation"; reprocess?: null | undefined | null };
};

export type TorrentSetTagsMutationVariables = Exact<{
  infoHashes: Array<Scalars["Hash20"]["input"]> | Scalars["Hash20"]["input"];
  tagNames: Array<Scalars["String"]["input"]> | Scalars["String"]["input"];
}>;

export type TorrentSetTagsMutation = {
  __typename?: "Mutation";
  torrent: { __typename?: "TorrentMutation"; setTags?: null | undefined | null };
};

export type TorrentSuggestTagsQueryVariables = Exact<{
  input: SuggestTagsQueryInput;
}>;

export type TorrentSuggestTagsQuery = {
  __typename?: "Query";
  torrent: {
    __typename?: "TorrentQuery";
    suggestTags: {
      __typename?: "TorrentSuggestTagsResult";
      suggestions: Array<{ __typename?: "SuggestedTag"; name: string; count: number }>;
    };
  };
};

export type VersionQueryVariables = Exact<{ [key: string]: never }>;

export type VersionQuery = { __typename?: "Query"; version: string };

export class TypedDocumentString<TResult, TVariables>
  extends String
  implements DocumentTypeDecoration<TResult, TVariables>
{
  __apiType?: NonNullable<DocumentTypeDecoration<TResult, TVariables>["__apiType"]>;
  private value: string;
  public __meta__?: Record<string, any> | undefined;

  constructor(value: string, __meta__?: Record<string, any> | undefined) {
    super(value);
    this.value = value;
    this.__meta__ = __meta__;
  }

  override toString(): string & DocumentTypeDecoration<TResult, TVariables> {
    return this.value;
  }
}

export const CollapsePathsDocument = new TypedDocumentString(`
    query CollapsePaths($input: TorrentContentCollapsePathsInput!) {
  torrentContent {
    collapsePaths(input: $input) {
      groups {
        path
        infoHashes
      }
    }
  }
}
    `) as unknown as TypedDocumentString<CollapsePathsQuery, CollapsePathsQueryVariables>;
export const FileSearchDocument = new TypedDocumentString(`
    query FileSearch($input: FileSearchInput!) {
  torrentContent {
    fileSearch(input: $input) {
      totalCount
      hasNextPage
      items {
        infoHash
        index
        path
        extension
        size
        torrentContent {
          infoHash
          seeders
          leechers
          publishedAt
          updatedAt
          dhtLastSeenAt
          dhtSeenCount
          title
          torrent {
            name
            magnetUri
          }
        }
      }
    }
  }
}
    `) as unknown as TypedDocumentString<FileSearchQuery, FileSearchQueryVariables>;
export const HealthCheckDocument = new TypedDocumentString(`
    query HealthCheck {
  health {
    status
    checks {
      key
      status
      timestamp
      error
    }
  }
  workers {
    listAll {
      workers {
        key
        started
      }
    }
  }
}
    `) as unknown as TypedDocumentString<HealthCheckQuery, HealthCheckQueryVariables>;
export const PathTypeaheadDocument = new TypedDocumentString(`
    query PathTypeahead($input: PathTypeaheadInput!) {
  torrentContent {
    pathTypeahead(input: $input) {
      suggestions
    }
  }
}
    `) as unknown as TypedDocumentString<PathTypeaheadQuery, PathTypeaheadQueryVariables>;
export const QueueEnqueueReprocessTorrentsBatchDocument = new TypedDocumentString(`
    mutation QueueEnqueueReprocessTorrentsBatch($input: QueueEnqueueReprocessTorrentsBatchInput!) {
  queue {
    enqueueReprocessTorrentsBatch(input: $input)
  }
}
    `) as unknown as TypedDocumentString<
  QueueEnqueueReprocessTorrentsBatchMutation,
  QueueEnqueueReprocessTorrentsBatchMutationVariables
>;
export const QueueJobsDocument = new TypedDocumentString(`
    query QueueJobs($input: QueueJobsQueryInput!) {
  queue {
    jobs(input: $input) {
      items {
        id
        queue
        status
        payload
        priority
        retries
        maxRetries
        runAfter
        ranAt
        error
        createdAt
      }
      totalCount
      hasNextPage
      aggregations {
        queue {
          value
          label
          count
        }
        status {
          value
          label
          count
        }
      }
    }
  }
}
    `) as unknown as TypedDocumentString<QueueJobsQuery, QueueJobsQueryVariables>;
export const QueueMetricsDocument = new TypedDocumentString(`
    query QueueMetrics($input: QueueMetricsQueryInput!) {
  queue {
    metrics(input: $input) {
      buckets {
        queue
        status
        createdAtBucket
        ranAtBucket
        count
        latency
      }
    }
  }
}
    `) as unknown as TypedDocumentString<QueueMetricsQuery, QueueMetricsQueryVariables>;
export const QueuePurgeJobsDocument = new TypedDocumentString(`
    mutation QueuePurgeJobs($input: QueuePurgeJobsInput!) {
  queue {
    purgeJobs(input: $input)
  }
}
    `) as unknown as TypedDocumentString<QueuePurgeJobsMutation, QueuePurgeJobsMutationVariables>;
export const TorrentContentSearchDocument = new TypedDocumentString(`
    query TorrentContentSearch($cached: Boolean, $facets: TorrentContentFacetsInput, $hasNextPage: Boolean, $limit: Int, $orderBy: [TorrentContentOrderByInput!], $page: Int, $queryString: String, $totalCount: Boolean) {
  torrentContent {
    search(
      input: {cached: $cached, facets: $facets, hasNextPage: $hasNextPage, limit: $limit, orderBy: $orderBy, page: $page, queryString: $queryString, totalCount: $totalCount}
    ) {
      totalCount
      totalCountIsEstimate
      hasNextPage
      items {
        infoHash
        title
        contentType
        seeders
        leechers
        dhtSeenCount
        dhtFirstSeenAt
        dhtLastSeenAt
        publishedAt
        torrent {
          filesCount
          magnetUri
          name
          size
        }
      }
      aggregations {
        contentType {
          value
          label
          count
          isEstimate
        }
        torrentSource {
          value
          label
          count
          isEstimate
        }
        torrentTag {
          value
          label
          count
          isEstimate
        }
        torrentFileType {
          value
          label
          count
          isEstimate
        }
        language {
          value
          label
          count
          isEstimate
        }
        genre {
          value
          label
          count
          isEstimate
        }
        videoResolution {
          value
          label
          count
          isEstimate
        }
        videoSource {
          value
          label
          count
          isEstimate
        }
      }
    }
  }
}
    `) as unknown as TypedDocumentString<
  TorrentContentSearchQuery,
  TorrentContentSearchQueryVariables
>;
export const TorrentDeleteDocument = new TypedDocumentString(`
    mutation TorrentDelete($infoHashes: [Hash20!]!) {
  torrent {
    delete(infoHashes: $infoHashes)
  }
}
    `) as unknown as TypedDocumentString<TorrentDeleteMutation, TorrentDeleteMutationVariables>;
export const TorrentDeleteTagsDocument = new TypedDocumentString(`
    mutation TorrentDeleteTags($infoHashes: [Hash20!], $tagNames: [String!]) {
  torrent {
    deleteTags(infoHashes: $infoHashes, tagNames: $tagNames)
  }
}
    `) as unknown as TypedDocumentString<
  TorrentDeleteTagsMutation,
  TorrentDeleteTagsMutationVariables
>;
export const TorrentDetailDocument = new TypedDocumentString(`
    query TorrentDetail($infoHashes: [Hash20!]!, $limit: Int) {
  torrentContent {
    search(input: {infoHashes: $infoHashes, limit: $limit}) {
      items {
        infoHash
        title
        contentType
        seeders
        leechers
        dhtSeenCount
        dhtFirstSeenAt
        dhtLastSeenAt
        publishedAt
        languages {
          id
          name
        }
        episodes {
          label
        }
        torrent {
          name
          size
          filesStatus
          filesCount
          fileType
          magnetUri
          sources {
            key
            name
            seenCount
            firstSeenAt
            lastSeenAt
          }
        }
        content {
          title
          originalTitle
          releaseDate
          releaseYear
          overview
          voteAverage
          voteCount
          originalLanguage {
            id
            name
          }
          attributes {
            source
            key
            value
          }
          collections {
            type
            name
          }
          externalLinks {
            metadataSource {
              key
              name
            }
            url
          }
        }
      }
    }
  }
}
    `) as unknown as TypedDocumentString<TorrentDetailQuery, TorrentDetailQueryVariables>;
export const TorrentFilesDocument = new TypedDocumentString(`
    query TorrentFiles($hasNextPage: Boolean, $infoHashes: [Hash20!], $limit: Int, $orderBy: [TorrentFilesOrderByInput!], $totalCount: Boolean) {
  torrent {
    files(
      input: {hasNextPage: $hasNextPage, infoHashes: $infoHashes, limit: $limit, orderBy: $orderBy, totalCount: $totalCount}
    ) {
      totalCount
      hasNextPage
      items {
        index
        path
        fileType
        size
      }
    }
  }
}
    `) as unknown as TypedDocumentString<TorrentFilesQuery, TorrentFilesQueryVariables>;
export const TorrentMetricsDocument = new TypedDocumentString(`
    query TorrentMetrics($input: TorrentMetricsQueryInput!) {
  torrent {
    metrics(input: $input) {
      buckets {
        source
        updated
        bucket
        count
      }
    }
    listSources {
      sources {
        key
        name
      }
    }
  }
}
    `) as unknown as TypedDocumentString<TorrentMetricsQuery, TorrentMetricsQueryVariables>;
export const TorrentPutTagsDocument = new TypedDocumentString(`
    mutation TorrentPutTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {
  torrent {
    putTags(infoHashes: $infoHashes, tagNames: $tagNames)
  }
}
    `) as unknown as TypedDocumentString<TorrentPutTagsMutation, TorrentPutTagsMutationVariables>;
export const TorrentReprocessDocument = new TypedDocumentString(`
    mutation TorrentReprocess($input: TorrentReprocessInput!) {
  torrent {
    reprocess(input: $input)
  }
}
    `) as unknown as TypedDocumentString<
  TorrentReprocessMutation,
  TorrentReprocessMutationVariables
>;
export const TorrentSetTagsDocument = new TypedDocumentString(`
    mutation TorrentSetTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {
  torrent {
    setTags(infoHashes: $infoHashes, tagNames: $tagNames)
  }
}
    `) as unknown as TypedDocumentString<TorrentSetTagsMutation, TorrentSetTagsMutationVariables>;
export const TorrentSuggestTagsDocument = new TypedDocumentString(`
    query TorrentSuggestTags($input: SuggestTagsQueryInput!) {
  torrent {
    suggestTags(input: $input) {
      suggestions {
        name
        count
      }
    }
  }
}
    `) as unknown as TypedDocumentString<TorrentSuggestTagsQuery, TorrentSuggestTagsQueryVariables>;
export const VersionDocument = new TypedDocumentString(`
    query Version {
  version
}
    `) as unknown as TypedDocumentString<VersionQuery, VersionQueryVariables>;
