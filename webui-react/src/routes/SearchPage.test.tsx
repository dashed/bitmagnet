import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
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

describe("SearchPage search modes", () => {
  beforeEach(() => {
    window.__BITMAGNET_FLAGS__ = undefined;
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: SAVED_SEARCHES_STORAGE_KEY,
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
});
