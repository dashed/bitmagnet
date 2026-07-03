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
  "query TorrentContentSearch($input: TorrentContentSearchQueryInput!) {\n  torrentContent {\n    search(input: $input) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        seeders\n        leechers\n        publishedAt\n        torrent {\n          name\n          size\n        }\n      }\n    }\n  }\n}": typeof types.TorrentContentSearchDocument;
  "query Version {\n  version\n}": typeof types.VersionDocument;
};
const documents: Documents = {
  "query TorrentContentSearch($input: TorrentContentSearchQueryInput!) {\n  torrentContent {\n    search(input: $input) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        seeders\n        leechers\n        publishedAt\n        torrent {\n          name\n          size\n        }\n      }\n    }\n  }\n}":
    types.TorrentContentSearchDocument,
  "query Version {\n  version\n}": types.VersionDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query TorrentContentSearch($input: TorrentContentSearchQueryInput!) {\n  torrentContent {\n    search(input: $input) {\n      totalCount\n      totalCountIsEstimate\n      hasNextPage\n      items {\n        infoHash\n        title\n        seeders\n        leechers\n        publishedAt\n        torrent {\n          name\n          size\n        }\n      }\n    }\n  }\n}",
): typeof import("./graphql").TorrentContentSearchDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "query Version {\n  version\n}",
): typeof import("./graphql").VersionDocument;

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}
