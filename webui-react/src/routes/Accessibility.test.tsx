import { fireEvent, render, screen, within } from "@testing-library/react";
import { createMemoryHistory, RouterProvider } from "@tanstack/react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "../i18n/i18n";
import { createAppRouter } from "../appRouter";
import { execute } from "../graphql/client";
import {
  HealthCheckDocument,
  QueueJobsDocument,
  QueueMetricsDocument,
  TorrentContentSearchDocument,
  TorrentDetailDocument,
  TorrentFilesDocument,
  TorrentMetricsDocument,
  VersionDocument,
  type HealthCheckQuery,
  type QueueJobsQuery,
  type QueueMetricsQuery,
  type TorrentContentSearchQuery,
  type TorrentDetailQuery,
  type TorrentFilesQuery,
  type TorrentMetricsQuery,
  type VersionQuery,
} from "../graphql/generated/graphql";
import { AppProviders } from "../providers/AppProviders";
import { addSavedSearch } from "../searches/savedSearches";
import { formatViolations, runAxe } from "../test/axe";

vi.mock("../graphql/client", () => ({
  execute: vi.fn(),
}));

const executeMock = vi.mocked(execute);
const INFO_HASH = "0123456789abcdef0123456789abcdef01234567";
const RECENT_SEARCHES_STORAGE_KEY = "bitmagnet-recent-searches";
const SAVED_SEARCHES_STORAGE_KEY = "bitmagnet-saved-searches";

const dashboardHealthResponse = {
  health: {
    checks: [
      {
        error: null,
        key: "database",
        status: "up",
        timestamp: "2026-07-10T12:00:00Z",
      },
    ],
    status: "up",
  },
  workers: {
    listAll: {
      workers: [
        {
          key: "queue-worker",
          started: true,
        },
      ],
    },
  },
} satisfies HealthCheckQuery;

const queueJobsResponse = {
  queue: {
    jobs: {
      aggregations: {
        queue: [
          {
            count: 1,
            label: "Process torrent",
            value: "process_torrent",
          },
        ],
        status: [
          {
            count: 1,
            label: "Pending",
            value: "pending",
          },
        ],
      },
      hasNextPage: false,
      items: [
        {
          createdAt: "2026-07-10T11:45:00Z",
          error: null,
          id: "job-accessibility-1",
          maxRetries: 3,
          payload: '{"infoHash":"0123456789abcdef0123456789abcdef01234567"}',
          priority: 10,
          queue: "process_torrent",
          ranAt: null,
          retries: 0,
          runAfter: "2026-07-10T12:00:00Z",
          status: "pending",
        },
      ],
      totalCount: 1,
    },
  },
} satisfies QueueJobsQuery;

const queueMetricsResponse = {
  queue: {
    metrics: {
      buckets: [
        {
          count: 1,
          createdAtBucket: "2026-07-10T11:00:00Z",
          latency: "1.25",
          queue: "process_torrent",
          ranAtBucket: null,
          status: "pending",
        },
      ],
    },
  },
} satisfies QueueMetricsQuery;

const searchResponse = {
  torrentContent: {
    search: {
      aggregations: {
        contentType: [
          {
            count: 1,
            isEstimate: false,
            label: "Movie",
            value: "movie",
          },
        ],
        genre: [
          {
            count: 1,
            isEstimate: false,
            label: "Science fiction",
            value: "science-fiction",
          },
        ],
        language: [
          {
            count: 1,
            isEstimate: false,
            label: "English",
            value: "en",
          },
        ],
        torrentFileType: [
          {
            count: 1,
            isEstimate: false,
            label: "Video",
            value: "video",
          },
        ],
        torrentSource: [
          {
            count: 1,
            isEstimate: false,
            label: "DHT",
            value: "dht",
          },
        ],
        torrentTag: [
          {
            count: 1,
            isEstimate: false,
            label: "Featured",
            value: "featured",
          },
        ],
        videoResolution: [
          {
            count: 1,
            isEstimate: false,
            label: "1080p",
            value: "V1080p",
          },
        ],
        videoSource: [
          {
            count: 1,
            isEstimate: false,
            label: "Web download",
            value: "WEBDL",
          },
        ],
      },
      hasNextPage: false,
      items: [
        {
          contentType: "movie",
          dhtFirstSeenAt: "2026-07-09T10:00:00Z",
          dhtLastSeenAt: "2026-07-10T10:00:00Z",
          dhtSeenCount: 8,
          infoHash: INFO_HASH,
          leechers: 3,
          publishedAt: "2026-07-08T10:00:00Z",
          seeders: 42,
          title: "Accessibility Demo Torrent",
          torrent: {
            filesCount: 2,
            magnetUri: `magnet:?xt=urn:btih:${INFO_HASH}`,
            name: "Accessibility Demo Torrent",
            size: 1_500_000_000,
          },
        },
      ],
      totalCount: 1,
      totalCountIsEstimate: false,
    },
  },
} satisfies TorrentContentSearchQuery;

const emptySearchResponse = {
  torrentContent: {
    search: {
      aggregations: {
        contentType: [],
        genre: [],
        language: [],
        torrentFileType: [],
        torrentSource: [],
        torrentTag: [],
        videoResolution: [],
        videoSource: [],
      },
      hasNextPage: false,
      items: [],
      totalCount: 0,
      totalCountIsEstimate: false,
    },
  },
} satisfies TorrentContentSearchQuery;

let torrentSearchResponse: TorrentContentSearchQuery = searchResponse;

const torrentDetailResponse = {
  torrentContent: {
    search: {
      items: [
        {
          content: {
            attributes: [],
            collections: [
              {
                name: "Science fiction",
                type: "genre",
              },
            ],
            externalLinks: [
              {
                metadataSource: {
                  key: "tmdb",
                  name: "TMDB",
                },
                url: "https://www.themoviedb.org/movie/1",
              },
            ],
            originalLanguage: {
              id: "en",
              name: "English",
            },
            originalTitle: "Accessibility Demo Original",
            overview: "A populated torrent used to exercise the detail page.",
            releaseDate: "2026-07-01",
            releaseYear: 2026,
            title: "Accessibility Demo Torrent",
            voteAverage: 8.2,
            voteCount: 125,
          },
          contentType: "movie",
          dhtFirstSeenAt: "2026-07-09T10:00:00Z",
          dhtLastSeenAt: "2026-07-10T10:00:00Z",
          dhtSeenCount: 8,
          episodes: {
            label: "S01",
          },
          infoHash: INFO_HASH,
          languages: [
            {
              id: "en",
              name: "English",
            },
          ],
          leechers: 3,
          publishedAt: "2026-07-08T10:00:00Z",
          seeders: 42,
          title: "Accessibility Demo Torrent",
          torrent: {
            filesCount: 2,
            filesStatus: "multi",
            fileType: "video",
            magnetUri: `magnet:?xt=urn:btih:${INFO_HASH}`,
            name: "Accessibility.Demo.2026",
            size: 1_500_000_000,
            sources: [
              {
                firstSeenAt: "2026-07-09T10:00:00Z",
                key: "dht",
                lastSeenAt: "2026-07-10T10:00:00Z",
                name: "DHT",
                seenCount: 8,
              },
            ],
          },
        },
      ],
    },
  },
} satisfies TorrentDetailQuery;

const torrentFilesResponse = {
  torrent: {
    files: {
      hasNextPage: false,
      items: [
        {
          fileType: "video",
          index: 0,
          path: "Accessibility.Demo.2026/movie.mkv",
          size: 1_499_000_000,
        },
        {
          fileType: "subtitles",
          index: 1,
          path: "Accessibility.Demo.2026/movie.en.srt",
          size: 1_000_000,
        },
      ],
      totalCount: 2,
    },
  },
} satisfies TorrentFilesQuery;

const torrentMetricsResponse = {
  torrent: {
    listSources: {
      sources: [
        {
          key: "dht",
          name: "DHT",
        },
      ],
    },
    metrics: {
      buckets: [
        {
          bucket: "2026-07-10T11:00:00Z",
          count: 2,
          source: "dht",
          updated: false,
        },
      ],
    },
  },
} satisfies TorrentMetricsQuery;

const versionResponse = {
  version: "v0.0.0-test",
} satisfies VersionQuery;

async function renderAt(initialEntry: string) {
  const router = createAppRouter({
    history: createMemoryHistory({ initialEntries: [initialEntry] }),
  });

  await router.load();

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  );
}

async function expectNoSeriousOrCriticalViolations() {
  const violations = await runAxe(document.body);

  expect(violations, formatViolations(violations)).toEqual([]);
}

describe("page accessibility", () => {
  beforeEach(() => {
    window.__BITMAGNET_FLAGS__ = undefined;
    torrentSearchResponse = searchResponse;
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: SAVED_SEARCHES_STORAGE_KEY,
      }),
    );
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: RECENT_SEARCHES_STORAGE_KEY,
      }),
    );
    executeMock.mockReset();
    executeMock.mockImplementation((document) => {
      if (document === HealthCheckDocument) {
        return Promise.resolve(dashboardHealthResponse);
      }

      if (document === QueueJobsDocument) {
        return Promise.resolve(queueJobsResponse);
      }

      if (document === QueueMetricsDocument) {
        return Promise.resolve(queueMetricsResponse);
      }

      if (document === TorrentContentSearchDocument) {
        return Promise.resolve(torrentSearchResponse);
      }

      if (document === TorrentDetailDocument) {
        return Promise.resolve(torrentDetailResponse);
      }

      if (document === TorrentFilesDocument) {
        return Promise.resolve(torrentFilesResponse);
      }

      if (document === TorrentMetricsDocument) {
        return Promise.resolve(torrentMetricsResponse);
      }

      if (document === VersionDocument) {
        return Promise.resolve(versionResponse);
      }

      throw new Error(`Unexpected GraphQL operation: ${document.toString()}`);
    });
  });

  it(
    "has no serious or critical violations on search results with filters open",
    async () => {
      await renderAt("/app/?content_type=movie&query=accessibility");

      expect(
        await screen.findByRole("link", { name: "Accessibility Demo Torrent" }),
      ).toBeTruthy();

      // The filters panel is a native <details>/<summary> disclosure. jsdom
      // does not toggle <details> on summary click, so open it directly —
      // otherwise the panel content never enters the accessibility tree.
      const filtersSummary = Array.from(
        document.querySelectorAll("details > summary"),
      ).find((el) => el.textContent?.startsWith("Filters"));
      expect(filtersSummary).toBeTruthy();
      const filtersDetails = filtersSummary!.closest("details")!;
      filtersDetails.open = true;
      fireEvent(filtersDetails, new Event("toggle"));

      // Each facet group is a nested <details>/<summary> — open them directly
      // (jsdom does not toggle <details> on click).
      for (const facetName of [
        "File type",
        "Genre",
        "Language",
        "Torrent source",
        "Torrent tag",
        "Video resolution",
        "Video source",
      ]) {
        const facetSummary = Array.from(
          filtersDetails.querySelectorAll("details > summary"),
        ).find((el) => el.textContent?.includes(facetName));
        expect(facetSummary, `facet group ${facetName}`).toBeTruthy();
        const facetDetails = facetSummary!.closest("details")!;
        facetDetails.open = true;
        fireEvent(facetDetails, new Event("toggle"));
      }

      expect(await screen.findByRole("checkbox", { name: /^Science fiction/ })).toBeTruthy();
      expect(screen.getByRole("checkbox", { name: /^English/ })).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations with active-filter chips visible",
    async () => {
      await renderAt(
        "/app/?content_type=movie&genre=science-fiction&published_at=7d&query=accessibility",
      );

      expect(await screen.findByRole("group", { name: "Filters applied" })).toBeTruthy();
      expect(
        screen.getByRole("button", { name: "Remove Genre: science-fiction filter" }),
      ).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations in the helpful zero-result state",
    async () => {
      torrentSearchResponse = emptySearchResponse;
      await renderAt(
        "/app/?content_type=movie&query=%22Missing.Torrent-2026%21%21%21%22",
      );

      expect(
        await screen.findByRole("heading", { level: 1, name: "No matching torrents" }),
      ).toBeTruthy();
      expect(screen.getByRole("button", { name: "Clear filters and retry" })).toBeTruthy();
      expect(
        screen.getByRole("button", {
          name: "Search instead for “Missing Torrent 2026”",
        }),
      ).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations on a populated torrent detail",
    async () => {
      await renderAt(`/app/torrents/${INFO_HASH}`);

      expect(
        await screen.findByRole("heading", { level: 1, name: "Accessibility Demo Torrent" }),
      ).toBeTruthy();
      expect(await screen.findByText("Accessibility.Demo.2026/movie.mkv")).toBeTruthy();
      expect(screen.getByRole("button", { name: "Copy" })).toBeTruthy();
      expect(screen.getByRole("button", { name: "Copy hash" })).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations on the dashboard",
    async () => {
      await renderAt("/app/dashboard");

      expect(await screen.findByRole("heading", { level: 1, name: "Dashboard" })).toBeTruthy();
      expect(await screen.findByRole("heading", { name: "Queue throughput" })).toBeTruthy();
      expect(await screen.findByRole("heading", { name: "Torrent throughput" })).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations on queue jobs and administration",
    async () => {
      await renderAt("/app/queue");

      expect(await screen.findByRole("heading", { level: 1, name: "Queue" })).toBeTruthy();
      expect(await screen.findByRole("heading", { name: "Jobs" })).toBeTruthy();
      expect(await screen.findByRole("heading", { name: "Admin" })).toBeTruthy();
      expect(await screen.findByText("job-accessibility-1")).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations in the open command palette",
    async () => {
      await renderAt("/app/");
      await screen.findByLabelText("Search torrents");

      fireEvent.keyDown(document.body, { key: "k", metaKey: true });

      const input = await screen.findByRole("combobox", { name: "Command palette" });
      const listbox = screen.getByRole("listbox");

      expect(input.getAttribute("aria-controls")).toBe(listbox.id);
      expect(within(listbox).getAllByRole("option").length).toBeGreaterThan(0);

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );

  it(
    "has no serious or critical violations with saved-search surfaces open",
    async () => {
      addSavedSearch("Accessibility favourites", {
        content_type: "movie",
        query: "accessibility",
      });
      await renderAt("/app/?query=accessibility");
      await screen.findByLabelText("Search torrents");

      // The trigger is a native <details>/<summary> disclosure. jsdom does not
      // toggle <details> on summary click, so open it directly — otherwise the
      // menu content never enters the accessibility tree.
      const savedSearchesSummary = Array.from(
        document.querySelectorAll("details > summary"),
      ).find((el) => el.textContent?.includes("Saved searches"));
      expect(savedSearchesSummary).toBeTruthy();
      const savedSearchesDetails = savedSearchesSummary!.closest("details")!;
      savedSearchesDetails.open = true;
      fireEvent(savedSearchesDetails, new Event("toggle"));
      expect(
        await screen.findByRole("button", { name: "Accessibility favourites" }),
      ).toBeTruthy();

      fireEvent.click(screen.getByRole("button", { name: "Save search" }));

      expect(await screen.findByRole("dialog", { name: "Save search" })).toBeTruthy();

      await expectNoSeriousOrCriticalViolations();
    },
    15_000,
  );
});
