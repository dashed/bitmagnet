import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { createMemoryHistory, RouterProvider } from "@tanstack/react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "../i18n/i18n";
import { createAppRouter } from "../appRouter";
import { execute } from "../graphql/client";
import { AppProviders } from "../providers/AppProviders";
import { PATH_TYPEAHEAD_DEBOUNCE_MS } from "./searchModes/PathBrowseView";

vi.mock("../graphql/client", () => ({
  execute: vi.fn(),
}));

const executeMock = vi.mocked(execute);

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
});
