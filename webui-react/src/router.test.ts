import { createMemoryHistory } from "@tanstack/react-router";
import { describe, expect, it } from "vitest";

import { createAppRouter } from "./appRouter";

const INFO_HASH = "0123456789abcdef0123456789abcdef01234567";

async function loadRouterAt(initialEntry: string) {
  const router = createAppRouter({
    history: createMemoryHistory({ initialEntries: [initialEntry] }),
  });

  await router.load();

  return router;
}

describe("legacy route redirects", () => {
  it("redirects legacy permalink paths to the React torrent detail route", async () => {
    const router = await loadRouterAt(`/app/torrents/permalink/${INFO_HASH}`);

    expect(router.latestLocation.pathname).toBe(`/torrents/${INFO_HASH}`);
    expect(router.latestLocation.search).toEqual({});
  });

  it("redirects valid legacy torrent search params to the React torrent detail route", async () => {
    const router = await loadRouterAt(
      `/app/torrents?query=matrix&torrent=${INFO_HASH}&tab=files&content_type=movie`,
    );

    expect(router.latestLocation.pathname).toBe(`/torrents/${INFO_HASH}`);
    expect(router.latestLocation.search).toEqual({});
  });

  it("strips malformed legacy torrent search params and stays on search", async () => {
    const router = await loadRouterAt(
      "/app/torrents?query=matrix&torrent=not-a-hash&tab=delete&content_type=movie",
    );

    expect(router.latestLocation.pathname).toBe("/");
    expect(router.latestLocation.search).toEqual({
      content_type: "movie",
      query: "matrix",
    });
  });

  it("round-trips a full legacy search URL through route search validation", async () => {
    const router = await loadRouterAt(
      [
        "/app/torrents?query=matrix%20resurrections",
        "content_type=movie",
        "order=published_at",
        "desc=0",
        "page=3",
        "limit=40",
        "facets=genre,language",
        "genre=sci-fi,action",
        "language=en,ja",
        "torrent_source=dht",
        "torrent_tag=freeleech",
        "file_type=video",
        "video_resolution=V1080p,null",
        "video_source=BluRay,null",
        "min_size=700",
        "min_size_unit=MiB",
        "max_size=2",
        "max_size_unit=GiB",
        "published_at=7d",
      ].join("&"),
    );
    const expectedSearch = {
      content_type: "movie",
      desc: 0,
      file_type: "video",
      genre: "action,sci-fi",
      language: "en,ja",
      limit: 40,
      max_size: 2,
      max_size_unit: "GiB",
      min_size: 700,
      min_size_unit: "MiB",
      order: "published_at",
      page: 3,
      published_at: "7d",
      query: "matrix resurrections",
      torrent_source: "dht",
      torrent_tag: "freeleech",
      video_resolution: "V1080p,null",
      video_source: "BluRay,null",
    };

    expect(router.latestLocation.pathname).toBe("/");
    expect(router.latestLocation.search).toEqual(expectedSearch);

    const roundTrip = router.buildLocation({
      search: router.latestLocation.search,
      to: "/",
    });

    expect(roundTrip.search).toEqual(expectedSearch);
    expect(roundTrip.search).not.toHaveProperty("facets");
    expect(roundTrip.searchStr).not.toContain("%7B");
  });
});
