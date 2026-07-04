import { describe, expect, it } from "vitest";

import {
  getTorrentSearchFacets,
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
