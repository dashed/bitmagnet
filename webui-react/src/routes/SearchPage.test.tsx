import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { createMemoryHistory, RouterProvider } from "@tanstack/react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "../i18n/i18n";
import { createAppRouter } from "../appRouter";
import { execute } from "../graphql/client";
import { AppProviders } from "../providers/AppProviders";
import { getSavedSearchesSnapshot } from "../searches/savedSearches";
import { PATH_TYPEAHEAD_DEBOUNCE_MS } from "./searchModes/PathBrowseView";

vi.mock("../graphql/client", () => ({
  execute: vi.fn(),
}));

const executeMock = vi.mocked(execute);
const RECENT_SEARCHES_STORAGE_KEY = "bitmagnet-recent-searches";
const SAVED_SEARCHES_STORAGE_KEY = "bitmagnet-saved-searches";

function getEmptyTorrentSearchResult() {
  return {
    torrentContent: {
      search: {
        aggregations: {
          contentType: [],
        },
        hasNextPage: false,
        items: [],
        totalCount: 0,
        totalCountIsEstimate: false,
      },
    },
  };
}

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

  return router;
}

describe("SearchPage", () => {
  beforeEach(() => {
    window.__BITMAGNET_FLAGS__ = undefined;
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
    executeMock.mockImplementation((_document, variables) => {
      const input = "input" in variables ? variables.input : undefined;

      if (input && typeof input === "object" && "prefix" in input) {
        return Promise.resolve({
          torrentContent: {
            pathTypeahead: {
              suggestions: ["movies/action", "movies/drama"],
            },
          },
        });
      }

      if (input && typeof input === "object" && "queryString" in input) {
        return Promise.resolve({
          torrentContent: {
            collapsePaths: {
              groups: [],
            },
          },
        });
      }

      if (input && typeof input === "object" && "query" in input) {
        return Promise.resolve({
          torrentContent: {
            fileSearch: {
              hasNextPage: false,
              items: [],
              totalCount: 0,
              totalCountIsEstimate: false,
            },
          },
        });
      }

      return Promise.resolve(getEmptyTorrentSearchResult());
    });
  });

  it("hides the search-mode switch when the runtime flag is off", async () => {
    window.__BITMAGNET_FLAGS__ = { enableSearchModes: false };

    await renderAt("/app/?mode=files");

    expect(await screen.findByLabelText("Search torrents")).toBeTruthy();
    expect(screen.queryByRole("navigation", { name: "Search modes" })).toBeNull();
  });

  it("debounces path typeahead requests by 250ms", async () => {
    await renderAt("/app/?mode=paths");

    const input = await screen.findByRole("combobox", { name: "Browse paths" });

    vi.useFakeTimers();

    try {
      fireEvent.change(input, { target: { value: "mov" } });

      act(() => {
        vi.advanceTimersByTime(PATH_TYPEAHEAD_DEBOUNCE_MS - 1);
      });

      expect(executeMock).not.toHaveBeenCalled();

      await act(async () => {
        await vi.advanceTimersByTimeAsync(1);
      });

      vi.useRealTimers();

      await waitFor(() => {
        expect(executeMock).toHaveBeenCalledTimes(1);
      });
      expect(executeMock.mock.calls[0]?.[1]).toEqual({
        input: {
          limit: 8,
          prefix: "mov",
        },
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not select or submit a path suggestion while Enter confirms IME composition", async () => {
    const router = await renderAt("/app/?mode=paths");
    const input = await screen.findByRole("combobox", { name: "Browse paths" });

    vi.useFakeTimers();

    try {
      fireEvent.change(input, { target: { value: "mov" } });

      await act(async () => {
        await vi.advanceTimersByTimeAsync(PATH_TYPEAHEAD_DEBOUNCE_MS);
      });

      vi.useRealTimers();

      const suggestion = await screen.findByRole("option", { name: "movies/action" });
      fireEvent.keyDown(input, { key: "ArrowDown" });

      expect(input.getAttribute("aria-activedescendant")).toBe(suggestion.id);
      expect(fireEvent.keyDown(input, { isComposing: true, key: "Enter" })).toBe(true);
      expect((input as HTMLInputElement).value).toBe("mov");
      expect(router.latestLocation.search).toEqual({ mode: "paths" });
      expect(screen.getByRole("option", { name: "movies/action" })).toBe(suggestion);
    } finally {
      vi.useRealTimers();
    }
  });

  it("writes custom published ranges and torrent page size to URL search params", async () => {
    const router = await renderAt("/app/?page=3&published_at=7d");

    fireEvent.change(await screen.findByLabelText("Published"), {
      target: { value: "__custom_range" },
    });

    fireEvent.change(await screen.findByLabelText("From"), {
      target: { value: "2023-01-01" },
    });
    fireEvent.change(screen.getByLabelText("To"), {
      target: { value: "2023-01-31" },
    });

    await waitFor(() => {
      expect(router.latestLocation.search).toMatchObject({
        published_at: "Jan 1, 2023 to Jan 31, 2023",
      });
      expect(router.latestLocation.search).not.toHaveProperty("page");
    });

    fireEvent.change(screen.getByLabelText("Per page"), {
      target: { value: "50" },
    });

    await waitFor(() => {
      expect(router.latestLocation.search).toMatchObject({
        limit: 50,
        published_at: "Jan 1, 2023 to Jan 31, 2023",
      });
      expect(router.latestLocation.search).not.toHaveProperty("page");
    });
  });

  it("writes files page size to the shared limit URL search param", async () => {
    const router = await renderAt("/app/?mode=files&query=sample&page=2");

    await screen.findByLabelText("Search files");
    fireEvent.change(await screen.findByLabelText("Per page"), {
      target: { value: "100" },
    });

    await waitFor(() => {
      expect(router.latestLocation.search).toEqual({
        limit: 100,
        mode: "files",
        query: "sample",
      });
    });
  });

  it("saves and reapplies the exact canonical URL search params", async () => {
    const router = await renderAt(
      "/app/?content_type=movie&desc=0&genre=sci-fi,action&limit=50&max_size=2&max_size_unit=GiB&min_size=700&min_size_unit=MiB&order=published_at&page=3&published_at=7d&query=matrix&video_resolution=V1080p",
    );
    const initialParams = { ...router.latestLocation.search };

    fireEvent.click(await screen.findByRole("button", { name: "Save search" }));

    const nameInput = await screen.findByLabelText("Name");
    expect((nameInput as HTMLInputElement).value).toBe("matrix");

    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByText("Search saved")).toBeTruthy();

    const searchInput = screen.getByLabelText("Search torrents");
    fireEvent.change(searchInput, { target: { value: "different" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => {
      expect(router.latestLocation.search).toMatchObject({ query: "different" });
    });

    fireEvent.click(screen.getByLabelText("Saved searches"));
    fireEvent.click(await screen.findByRole("button", { name: "matrix" }));

    await waitFor(() => {
      expect(router.latestLocation.search).toEqual(initialParams);
      expect(getSavedSearchesSnapshot()[0]?.params).toEqual(router.latestLocation.search);
    });
  });

  it("renders active-filter chips and removes only the dismissed facet value", async () => {
    const router = await renderAt(
      "/app/?content_type=movie&genre=action&language=en&max_size=2&max_size_unit=GiB&min_size=700&min_size_unit=MiB&published_at=7d&query=matrix",
    );

    expect(await screen.findByRole("group", { name: "Filters applied" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Remove Movie filter" })).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Remove 700 MiB – 2 GiB filter" }),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "Remove Last week filter" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Remove Genre: action filter" }));

    await waitFor(() => {
      expect(router.latestLocation.search).toEqual({
        content_type: "movie",
        language: "en",
        max_size: 2,
        max_size_unit: "GiB",
        min_size: 700,
        min_size_unit: "MiB",
        published_at: "7d",
        query: "matrix",
      });
    });
  });

  it(
    "offers filter recovery and a simplified query when constrained results are empty",
    async () => {
      const router = await renderAt(
        "/app/?content_type=movie&query=%22The.Matrix-Reloaded%21%21%21%22",
      );

      expect(
        await screen.findByRole("heading", { level: 1, name: "No matching torrents" }),
      ).toBeTruthy();
      expect(screen.getByRole("button", { name: "Clear filters and retry" })).toBeTruthy();
      expect(
        screen.getByRole("button", {
          name: "Search instead for “The Matrix Reloaded”",
        }),
      ).toBeTruthy();

      fireEvent.click(screen.getByRole("button", { name: "Clear filters and retry" }));

      await waitFor(() => {
        expect(router.latestLocation.search).toEqual({ query: "The.Matrix-Reloaded!!!" });
      });

      fireEvent.click(
        screen.getByRole("button", {
          name: "Search instead for “The Matrix Reloaded”",
        }),
      );

      await waitFor(() => {
        expect(router.latestLocation.search).toEqual({ query: "The Matrix Reloaded" });
      });
    },
  );

  it("records a submitted query and shows it in Recent after reloading", async () => {
    const router = await renderAt("/app/");

    fireEvent.change(await screen.findByLabelText("Search torrents"), {
      target: { value: "  Ubuntu  " },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => {
      expect(router.latestLocation.search).toEqual({ query: "Ubuntu" });
    });

    cleanup();
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: RECENT_SEARCHES_STORAGE_KEY,
      }),
    );
    await renderAt("/app/");

    expect(await screen.findByRole("region", { name: "Recent" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Ubuntu" })).toBeTruthy();
  });

  it("shows the exact summed contentType count, not the totalCount estimate", async () => {
    executeMock.mockImplementation((_document, variables) => {
      if ("input" in variables) {
        return Promise.resolve(getEmptyTorrentSearchResult());
      }

      return Promise.resolve({
        torrentContent: {
          search: {
            aggregations: {
              contentType: [
                { count: 1, isEstimate: false, label: "ebook", value: "ebook" },
                { count: 1, isEstimate: false, label: "movie", value: "movie" },
                { count: 2, isEstimate: false, label: "Unknown", value: null },
              ],
            },
            hasNextPage: false,
            items: [
              {
                contentType: "movie",
                dhtFirstSeenAt: "2024-01-01T00:00:00Z",
                dhtLastSeenAt: "2024-01-02T00:00:00Z",
                dhtSeenCount: 5,
                infoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                leechers: 2,
                publishedAt: "2024-01-01T00:00:00Z",
                seeders: 10,
                title: "The Lehman Trilogy",
                torrent: {
                  fileExtensions: ["mp4"],
                  filesCount: 3,
                  magnetUri: "magnet:?xt=urn:btih:aaaa",
                  name: "The.Lehman.Trilogy.2019.1080p",
                  singleFile: false,
                  size: 1234567,
                },
              },
            ],
            // Bogus budgeted_count planner estimate: the UI must not surface this.
            totalCount: 669,
            totalCountIsEstimate: true,
          },
        },
      });
    });

    await renderAt("/app/?query=lehman+trilogy");

    expect(await screen.findByText("4 results")).toBeTruthy();
    expect(screen.queryByText("About 669 results")).toBeNull();
  });

  it("labels the files-mode headline as an estimate when totalCount is approximate", async () => {
    executeMock.mockImplementation((_document, variables) => {
      const input = "input" in variables ? variables.input : undefined;

      if (input && typeof input === "object" && "query" in input) {
        return Promise.resolve({
          torrentContent: {
            fileSearch: {
              hasNextPage: false,
              items: [],
              // L3 candidate_total: a torrent-doc recall upper bound, not an
              // exact matching-file count — the UI must not present it as exact.
              totalCount: 669,
              totalCountIsEstimate: true,
            },
          },
        });
      }

      return Promise.resolve(getEmptyTorrentSearchResult());
    });

    await renderAt("/app/?mode=files&query=lehman+trilogy");

    expect(await screen.findByText("About 669 files")).toBeTruthy();
    expect(screen.queryByText("669 files")).toBeNull();
  });

  it("shows an exact files-mode headline when totalCount is not an estimate", async () => {
    executeMock.mockImplementation((_document, variables) => {
      const input = "input" in variables ? variables.input : undefined;

      if (input && typeof input === "object" && "query" in input) {
        return Promise.resolve({
          torrentContent: {
            fileSearch: {
              hasNextPage: false,
              items: [],
              totalCount: 11,
              totalCountIsEstimate: false,
            },
          },
        });
      }

      return Promise.resolve(getEmptyTorrentSearchResult());
    });

    await renderAt("/app/?mode=files&query=exact+file+count");

    expect(await screen.findByText("11 files")).toBeTruthy();
    expect(screen.queryByText("About 11 files")).toBeNull();
  });

  it("defaults the torrent search page size to 20 (matches Angular; not the slow 100)", async () => {
    await renderAt("/app/?query=ubuntu");

    await waitFor(() => {
      const torrentCall = executeMock.mock.calls.find(
        ([, variables]) =>
          variables && !("input" in variables) && "queryString" in variables,
      );

      expect(torrentCall).toBeTruthy();
      expect((torrentCall?.[1] as { limit?: number }).limit).toBe(20);
    });
  });
});
