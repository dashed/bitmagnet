import { beforeEach, describe, expect, it } from "vitest";

import {
  addSavedSearch,
  deleteSavedSearch,
  getSavedSearchesSnapshot,
  renameSavedSearch,
} from "./savedSearches";

const SAVED_SEARCHES_STORAGE_KEY = "bitmagnet-saved-searches";

function invalidateSavedSearchesSnapshot() {
  window.dispatchEvent(
    new StorageEvent("storage", {
      key: SAVED_SEARCHES_STORAGE_KEY,
    }),
  );
}

function setStoredValue(value: string) {
  window.localStorage.setItem(SAVED_SEARCHES_STORAGE_KEY, value);
  invalidateSavedSearchesSnapshot();
}

describe("saved searches", () => {
  beforeEach(() => {
    invalidateSavedSearchesSnapshot();
  });

  it("adds searches and overwrites a case-insensitive name match", () => {
    const first = addSavedSearch("  Linux  ", { query: "debian" });

    expect(first).toBeDefined();
    expect(getSavedSearchesSnapshot()).toEqual([
      expect.objectContaining({
        name: "Linux",
        params: { query: "debian" },
      }),
    ]);

    const overwritten = addSavedSearch("linux", {
      content_type: "software",
      query: "arch",
    });
    const snapshot = getSavedSearchesSnapshot();

    expect(snapshot).toHaveLength(1);
    expect(overwritten?.id).toBe(first?.id);
    expect(snapshot[0]).toEqual(
      expect.objectContaining({
        name: "linux",
        params: {
          content_type: "software",
          query: "arch",
        },
      }),
    );
    expect(addSavedSearch("   ", { query: "ignored" })).toBeUndefined();
    expect(getSavedSearchesSnapshot()).toHaveLength(1);
  });

  it("renames and deletes a saved search", () => {
    const savedSearch = addSavedSearch("Linux", { query: "debian" });

    expect(savedSearch).toBeDefined();
    expect(renameSavedSearch(savedSearch?.id ?? "", "  Distros  ")?.name).toBe("Distros");
    invalidateSavedSearchesSnapshot();
    expect(getSavedSearchesSnapshot()[0]?.name).toBe("Distros");

    deleteSavedSearch(savedSearch?.id ?? "");
    invalidateSavedSearchesSnapshot();

    expect(getSavedSearchesSnapshot()).toEqual([]);
    expect(window.localStorage.getItem(SAVED_SEARCHES_STORAGE_KEY)).toBe(
      JSON.stringify({ items: [], version: 1 }),
    );
  });

  it("returns an empty list for malformed or unsupported storage", () => {
    const invalidValues = [
      "{",
      JSON.stringify({ items: [], version: 2 }),
      JSON.stringify({ items: {}, version: 1 }),
    ];

    for (const invalidValue of invalidValues) {
      setStoredValue(invalidValue);

      expect(() => getSavedSearchesSnapshot()).not.toThrow();
      expect(getSavedSearchesSnapshot()).toEqual([]);
    }
  });

  it("drops invalid items and canonicalizes valid params", () => {
    setStoredValue(
      JSON.stringify({
        items: [
          {
            createdAt: 1,
            id: "valid",
            name: "Movies",
            params: {
              content_type: "movie",
              junk: "drop-me",
              page: "2",
              query: "  matrix  ",
            },
          },
          {
            createdAt: "yesterday",
            id: "invalid-created-at",
            name: "Invalid",
            params: {},
          },
          {
            createdAt: 2,
            id: 42,
            name: "Invalid",
            params: {},
          },
          {
            createdAt: 3,
            id: "invalid-params",
            name: "Invalid",
            params: [],
          },
          {
            createdAt: 4,
            id: "invalid-name",
            name: 42,
            params: {},
          },
        ],
        version: 1,
      }),
    );

    expect(getSavedSearchesSnapshot()).toEqual([
      {
        createdAt: 1,
        id: "valid",
        name: "Movies",
        params: {
          content_type: "movie",
          page: 2,
          query: "matrix",
        },
      },
    ]);
  });

  it("returns a stable snapshot reference until storage changes", () => {
    const firstSnapshot = getSavedSearchesSnapshot();

    expect(getSavedSearchesSnapshot()).toBe(firstSnapshot);

    setStoredValue(JSON.stringify({ items: [], version: 1 }));

    expect(getSavedSearchesSnapshot()).not.toBe(firstSnapshot);
  });
});
