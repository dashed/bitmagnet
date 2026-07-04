/* eslint-disable */
import * as types from "./graphql";

/**
 * Map of all GraphQL operations in the project.
 *
 * This map has several performance disadvantages:
 * 1. It is not tree-shakeable, so it will include all operations in the project.
 * 2. It is not minifiable, so the string of a GraphQL query will be multiple times inside the bundle.
 * 3. It does not support dead code elimination, so it will add unused operations.
 *
 * Therefore it is highly recommended to use the babel or swc plugin for production.
 * Learn more about it here: https://the-guild.dev/graphql/codegen/plugins/presets/preset-client#reducing-bundle-size
 */
type Documents = {
  "query TorrentContentSearch($cached: Boolean, $facets: TorrentContentFacetsInput, $hasNextPage: Boolean, $limit: Int, $orderBy: [TorrentContentOrderByInput!], $page: Int, $queryString: String, $totalCount: Boolean) {\n  torrentContent {\n    search(\n      input: {cached: $cached, facets: $facets, hasNextPage: $hasNextPage, limit: $limit, orderBy: $orderBy, page: $page, queryString: $queryString, totalCount: $totalCount}\n    ) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        torrent {\n          filesCount\n          magnetUri\n          name\n          size\n        }\n      }\n      aggregations {\n        contentType {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentSource {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentTag {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentFileType {\n          value\n          label\n          count\n          isEstimate\n        }\n        language {\n          value\n          label\n          count\n          isEstimate\n        }\n        genre {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoResolution {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoSource {\n          value\n          label\n          count\n          isEstimate\n        }\n      }\n    }\n  }\n}": typeof types.TorrentContentSearchDocument;
  "mutation TorrentDelete($infoHashes: [Hash20!]!) {\n  torrent {\n    delete(infoHashes: $infoHashes)\n  }\n}": typeof types.TorrentDeleteDocument;
  "mutation TorrentDeleteTags($infoHashes: [Hash20!], $tagNames: [String!]) {\n  torrent {\n    deleteTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}": typeof types.TorrentDeleteTagsDocument;
  "query TorrentDetail($infoHashes: [Hash20!]!, $limit: Int) {\n  torrentContent {\n    search(input: {infoHashes: $infoHashes, limit: $limit}) {\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        languages {\n          id\n          name\n        }\n        episodes {\n          label\n        }\n        torrent {\n          name\n          size\n          filesStatus\n          filesCount\n          fileType\n          magnetUri\n          sources {\n            key\n            name\n            seenCount\n            firstSeenAt\n            lastSeenAt\n          }\n        }\n        content {\n          title\n          originalTitle\n          releaseDate\n          releaseYear\n          overview\n          voteAverage\n          voteCount\n          originalLanguage {\n            id\n            name\n          }\n          attributes {\n            source\n            key\n            value\n          }\n          collections {\n            type\n            name\n          }\n          externalLinks {\n            metadataSource {\n              key\n              name\n            }\n            url\n          }\n        }\n      }\n    }\n  }\n}": typeof types.TorrentDetailDocument;
  "query TorrentFiles($hasNextPage: Boolean, $infoHashes: [Hash20!], $limit: Int, $orderBy: [TorrentFilesOrderByInput!], $totalCount: Boolean) {\n  torrent {\n    files(\n      input: {hasNextPage: $hasNextPage, infoHashes: $infoHashes, limit: $limit, orderBy: $orderBy, totalCount: $totalCount}\n    ) {\n      totalCount\n      hasNextPage\n      items {\n        index\n        path\n        fileType\n        size\n      }\n    }\n  }\n}": typeof types.TorrentFilesDocument;
  "mutation TorrentPutTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    putTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}": typeof types.TorrentPutTagsDocument;
  "mutation TorrentReprocess($input: TorrentReprocessInput!) {\n  torrent {\n    reprocess(input: $input)\n  }\n}": typeof types.TorrentReprocessDocument;
  "mutation TorrentSetTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    setTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}": typeof types.TorrentSetTagsDocument;
  "query TorrentSuggestTags($input: SuggestTagsQueryInput!) {\n  torrent {\n    suggestTags(input: $input) {\n      suggestions {\n        name\n        count\n      }\n    }\n  }\n}": typeof types.TorrentSuggestTagsDocument;
  "query Version {\n  version\n}": typeof types.VersionDocument;
};
const documents: Documents = {
  "query TorrentContentSearch($cached: Boolean, $facets: TorrentContentFacetsInput, $hasNextPage: Boolean, $limit: Int, $orderBy: [TorrentContentOrderByInput!], $page: Int, $queryString: String, $totalCount: Boolean) {\n  torrentContent {\n    search(\n      input: {cached: $cached, facets: $facets, hasNextPage: $hasNextPage, limit: $limit, orderBy: $orderBy, page: $page, queryString: $queryString, totalCount: $totalCount}\n    ) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        torrent {\n          filesCount\n          magnetUri\n          name\n          size\n        }\n      }\n      aggregations {\n        contentType {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentSource {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentTag {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentFileType {\n          value\n          label\n          count\n          isEstimate\n        }\n        language {\n          value\n          label\n          count\n          isEstimate\n        }\n        genre {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoResolution {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoSource {\n          value\n          label\n          count\n          isEstimate\n        }\n      }\n    }\n  }\n}":
    types.TorrentContentSearchDocument,
  "mutation TorrentDelete($infoHashes: [Hash20!]!) {\n  torrent {\n    delete(infoHashes: $infoHashes)\n  }\n}":
    types.TorrentDeleteDocument,
  "mutation TorrentDeleteTags($infoHashes: [Hash20!], $tagNames: [String!]) {\n  torrent {\n    deleteTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}":
    types.TorrentDeleteTagsDocument,
  "query TorrentDetail($infoHashes: [Hash20!]!, $limit: Int) {\n  torrentContent {\n    search(input: {infoHashes: $infoHashes, limit: $limit}) {\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        languages {\n          id\n          name\n        }\n        episodes {\n          label\n        }\n        torrent {\n          name\n          size\n          filesStatus\n          filesCount\n          fileType\n          magnetUri\n          sources {\n            key\n            name\n            seenCount\n            firstSeenAt\n            lastSeenAt\n          }\n        }\n        content {\n          title\n          originalTitle\n          releaseDate\n          releaseYear\n          overview\n          voteAverage\n          voteCount\n          originalLanguage {\n            id\n            name\n          }\n          attributes {\n            source\n            key\n            value\n          }\n          collections {\n            type\n            name\n          }\n          externalLinks {\n            metadataSource {\n              key\n              name\n            }\n            url\n          }\n        }\n      }\n    }\n  }\n}":
    types.TorrentDetailDocument,
  "query TorrentFiles($hasNextPage: Boolean, $infoHashes: [Hash20!], $limit: Int, $orderBy: [TorrentFilesOrderByInput!], $totalCount: Boolean) {\n  torrent {\n    files(\n      input: {hasNextPage: $hasNextPage, infoHashes: $infoHashes, limit: $limit, orderBy: $orderBy, totalCount: $totalCount}\n    ) {\n      totalCount\n      hasNextPage\n      items {\n        index\n        path\n        fileType\n        size\n      }\n    }\n  }\n}":
    types.TorrentFilesDocument,
  "mutation TorrentPutTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    putTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}":
    types.TorrentPutTagsDocument,
  "mutation TorrentReprocess($input: TorrentReprocessInput!) {\n  torrent {\n    reprocess(input: $input)\n  }\n}":
    types.TorrentReprocessDocument,
  "mutation TorrentSetTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    setTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}":
    types.TorrentSetTagsDocument,
  "query TorrentSuggestTags($input: SuggestTagsQueryInput!) {\n  torrent {\n    suggestTags(input: $input) {\n      suggestions {\n        name\n        count\n      }\n    }\n  }\n}":
    types.TorrentSuggestTagsDocument,
  "query Version {\n  version\n}": types.VersionDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query TorrentContentSearch($cached: Boolean, $facets: TorrentContentFacetsInput, $hasNextPage: Boolean, $limit: Int, $orderBy: [TorrentContentOrderByInput!], $page: Int, $queryString: String, $totalCount: Boolean) {\n  torrentContent {\n    search(\n      input: {cached: $cached, facets: $facets, hasNextPage: $hasNextPage, limit: $limit, orderBy: $orderBy, page: $page, queryString: $queryString, totalCount: $totalCount}\n    ) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        torrent {\n          filesCount\n          magnetUri\n          name\n          size\n        }\n      }\n      aggregations {\n        contentType {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentSource {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentTag {\n          value\n          label\n          count\n          isEstimate\n        }\n        torrentFileType {\n          value\n          label\n          count\n          isEstimate\n        }\n        language {\n          value\n          label\n          count\n          isEstimate\n        }\n        genre {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoResolution {\n          value\n          label\n          count\n          isEstimate\n        }\n        videoSource {\n          value\n          label\n          count\n          isEstimate\n        }\n      }\n    }\n  }\n}",
): typeof import("./graphql").TorrentContentSearchDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "mutation TorrentDelete($infoHashes: [Hash20!]!) {\n  torrent {\n    delete(infoHashes: $infoHashes)\n  }\n}",
): typeof import("./graphql").TorrentDeleteDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "mutation TorrentDeleteTags($infoHashes: [Hash20!], $tagNames: [String!]) {\n  torrent {\n    deleteTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}",
): typeof import("./graphql").TorrentDeleteTagsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query TorrentDetail($infoHashes: [Hash20!]!, $limit: Int) {\n  torrentContent {\n    search(input: {infoHashes: $infoHashes, limit: $limit}) {\n      items {\n        infoHash\n        title\n        contentType\n        seeders\n        leechers\n        dhtSeenCount\n        dhtFirstSeenAt\n        dhtLastSeenAt\n        publishedAt\n        languages {\n          id\n          name\n        }\n        episodes {\n          label\n        }\n        torrent {\n          name\n          size\n          filesStatus\n          filesCount\n          fileType\n          magnetUri\n          sources {\n            key\n            name\n            seenCount\n            firstSeenAt\n            lastSeenAt\n          }\n        }\n        content {\n          title\n          originalTitle\n          releaseDate\n          releaseYear\n          overview\n          voteAverage\n          voteCount\n          originalLanguage {\n            id\n            name\n          }\n          attributes {\n            source\n            key\n            value\n          }\n          collections {\n            type\n            name\n          }\n          externalLinks {\n            metadataSource {\n              key\n              name\n            }\n            url\n          }\n        }\n      }\n    }\n  }\n}",
): typeof import("./graphql").TorrentDetailDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query TorrentFiles($hasNextPage: Boolean, $infoHashes: [Hash20!], $limit: Int, $orderBy: [TorrentFilesOrderByInput!], $totalCount: Boolean) {\n  torrent {\n    files(\n      input: {hasNextPage: $hasNextPage, infoHashes: $infoHashes, limit: $limit, orderBy: $orderBy, totalCount: $totalCount}\n    ) {\n      totalCount\n      hasNextPage\n      items {\n        index\n        path\n        fileType\n        size\n      }\n    }\n  }\n}",
): typeof import("./graphql").TorrentFilesDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "mutation TorrentPutTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    putTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}",
): typeof import("./graphql").TorrentPutTagsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "mutation TorrentReprocess($input: TorrentReprocessInput!) {\n  torrent {\n    reprocess(input: $input)\n  }\n}",
): typeof import("./graphql").TorrentReprocessDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "mutation TorrentSetTags($infoHashes: [Hash20!]!, $tagNames: [String!]!) {\n  torrent {\n    setTags(infoHashes: $infoHashes, tagNames: $tagNames)\n  }\n}",
): typeof import("./graphql").TorrentSetTagsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query TorrentSuggestTags($input: SuggestTagsQueryInput!) {\n  torrent {\n    suggestTags(input: $input) {\n      suggestions {\n        name\n        count\n      }\n    }\n  }\n}",
): typeof import("./graphql").TorrentSuggestTagsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query Version {\n  version\n}",
): typeof import("./graphql").VersionDocument;

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}
