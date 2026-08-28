import { describe, expect, it } from "vitest";

import {
  formatPublishedRangeValue,
  getFileSearchSort,
  getPublishedRangeInputValues,
  getTorrentSearchFacets,
  isValidPublishedAtValue,
  normalizeLegacyTorrentSearch,
  parseTorrentSearchParams,
  stringifyTorrentSearchParams,
  validateTorrentSearchParams,
} from "./searchParams";

describe("torrent search params", () => {
  it("parses legacy-shaped params and elides dynamic defaults on round trip", () => {
    expect(stringifyTorrentSearchParams(parseTorrentSearchParams({}))).toEqual({});

    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          desc: "1",
          order: "relevance",
          query: "matrix",
        }),
      ),
    ).toEqual({ query: "matrix" });

    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          content_type: "movie",
          desc: "0",
          max_size: "2",
          max_size_unit: "GiB",
          min_size: "700",
          min_size_unit: "MiB",
          order: "published_at",
          page: "3",
          published_at: "7d",
          query: "matrix",
        }),
      ),
    ).toEqual({
      content_type: "movie",
      desc: 0,
      max_size: 2,
      max_size_unit: "GiB",
      min_size: 700,
      min_size_unit: "MiB",
      order: "published_at",
      page: 3,
      published_at: "7d",
      query: "matrix",
    });
  });

  it("round-trips dynamic facet params and builds selected facet filters", () => {
    const search = parseTorrentSearchParams({
      content_type: "movie",
      file_type: "video",
      genre: "sci-fi,action",
      language: "en,ja",
      torrent_source: "dht",
      torrent_tag: "freeleech",
      video_resolution: "V1080p,null",
      video_source: "BluRay,null",
    });

    expect(stringifyTorrentSearchParams(search)).toEqual({
      content_type: "movie",
      file_type: "video",
      genre: "action,sci-fi",
      language: "en,ja",
      torrent_source: "dht",
      torrent_tag: "freeleech",
      video_resolution: "V1080p,null",
      video_source: "BluRay,null",
    });

    expect(getTorrentSearchFacets(search)).toMatchObject({
      genre: {
        aggregate: true,
        filter: ["action", "sci-fi"],
      },
      torrentFileType: {
        aggregate: true,
        filter: ["video"],
      },
      torrentSource: {
        aggregate: true,
        filter: ["dht"],
      },
      torrentTag: {
        aggregate: true,
        filter: ["freeleech"],
      },
      videoResolution: {
        aggregate: true,
        filter: ["V1080p", null],
      },
      videoSource: {
        aggregate: true,
        filter: ["BluRay", null],
      },
    });
    expect(getTorrentSearchFacets(search).torrentTag).not.toHaveProperty("logic");
  });

  it("round-trips custom published date ranges through the legacy published_at param", () => {
    const publishedAt = "Jan 1, 2023 to Jan 31, 2023";
    const search = parseTorrentSearchParams({
      page: "4",
      published_at: publishedAt,
      query: "matrix",
    });

    expect(search.publishedAt).toBe(publishedAt);
    expect(getPublishedRangeInputValues(search.publishedAt)).toEqual({
      end: "2023-01-31",
      start: "2023-01-01",
    });
    expect(stringifyTorrentSearchParams(search)).toEqual({
      page: 4,
      published_at: publishedAt,
      query: "matrix",
    });
    expect(getTorrentSearchFacets(search).publishedAt).toBe(publishedAt);
    expect(formatPublishedRangeValue("2023-01-01", "2023-01-31")).toBe(publishedAt);
  });

  it("drops invalid published_at values instead of sending malformed backend filters", () => {
    const search = parseTorrentSearchParams({
      published: "2023-03-01 to 2023-01-01",
      published_at: "not a time frame",
      query: "matrix",
    });

    expect(search.publishedAt).toBeUndefined();
    expect(stringifyTorrentSearchParams(search)).toEqual({ query: "matrix" });
    expect(getTorrentSearchFacets(search)).not.toHaveProperty("publishedAt");
    expect(isValidPublishedAtValue("2023-01-01")).toBe(true);
    expect(isValidPublishedAtValue("3M")).toBe(true);
    expect(isValidPublishedAtValue("3mo")).toBe(false);
  });

  it("round-trips search modes while eliding the torrent default", () => {
    expect(stringifyTorrentSearchParams(parseTorrentSearchParams({ mode: "torrents" }))).toEqual(
      {},
    );
    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          mode: "files",
          query: "sample",
        }),
      ),
    ).toEqual({
      mode: "files",
      query: "sample",
    });
    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          mode: "paths",
          page: "2",
          query: "movies",
        }),
      ),
    ).toEqual({
      mode: "paths",
      page: 2,
      query: "movies",
    });
  });

  it("normalizes file-mode sort params and builds fileSearch sort input", () => {
    const lastSeenSearch = parseTorrentSearchParams({
      mode: "files",
      order: "last_seen",
      query: "sample",
    });

    expect(stringifyTorrentSearchParams(lastSeenSearch)).toEqual({
      desc: 1,
      mode: "files",
      order: "last_seen",
      query: "sample",
    });
    expect(getFileSearchSort(lastSeenSearch)).toEqual([
      {
        descending: true,
        field: "last_seen",
      },
    ]);

    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          mode: "files",
          order: "last_seen",
        }),
      ),
    ).toEqual({
      mode: "files",
    });

    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          mode: "files",
          order: "path",
        }),
      ),
    ).toEqual({
      desc: 0,
      mode: "files",
      order: "path",
    });
  });

  it("drops content-type-specific facet params when the content type changes", () => {
    const movieSearch = parseTorrentSearchParams({
      content_type: "movie",
      genre: "action",
      language: "en",
      torrent_source: "dht",
      video_resolution: "V1080p,null",
      video_source: "BluRay,null",
    });

    expect(
      stringifyTorrentSearchParams({
        ...movieSearch,
        contentType: "music",
      }),
    ).toEqual({
      content_type: "music",
      language: "en",
      torrent_source: "dht",
    });

    expect(
      stringifyTorrentSearchParams(
        parseTorrentSearchParams({
          content_type: "music",
          genre: "action",
          video_resolution: "V1080p",
          video_source: "BluRay",
        }),
      ),
    ).toEqual({
      content_type: "music",
    });
  });

  it("classifies valid legacy torrent search params for detail redirects", () => {
    const infoHash = "ABCDEFabcdef0123456789abcdef0123456789ab";

    expect(
      normalizeLegacyTorrentSearch({
        content_type: "movie",
        query: "matrix",
        tab: "files",
        torrent: infoHash,
      }),
    ).toEqual({
      infoHash: infoHash.toLowerCase(),
      kind: "detail",
    });
  });

  it("strips malformed legacy torrent search params", () => {
    const legacySearch = {
      content_type: "movie",
      query: "matrix",
      tab: "files",
      torrent: "not-a-hash",
    };

    expect(normalizeLegacyTorrentSearch(legacySearch)).toEqual({
      kind: "search",
      search: {
        content_type: "movie",
        query: "matrix",
      },
    });
    expect(validateTorrentSearchParams(legacySearch)).toEqual({
      content_type: "movie",
      query: "matrix",
    });
  });
});
