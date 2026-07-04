import { describe, expect, it } from "vitest";

import { parseTorrentSearchParams, stringifyTorrentSearchParams } from "./searchParams";

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
});
