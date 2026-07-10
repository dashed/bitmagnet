import { beforeEach, describe, expect, it } from "vitest";

import {
  clearRecentSearches,
  getRecentSearchesSnapshot,
  recordRecentSearch,
} from "./recentSearches";

const RECENT_SEARCHES_STORAGE_KEY = "bitmagnet-recent-searches";

function invalidateRecentSearchesSnapshot() {
  window.dispatchEvent(
    new StorageEvent("storage", {
      key: RECENT_SEARCHES_STORAGE_KEY,
    }),
  );
}

function setStoredValue(value: string) {
  window.localStorage.setItem(RECENT_SEARCHES_STORAGE_KEY, value);
  invalidateRecentSearchesSnapshot();
}

describe("recent searches", () => {
  beforeEach(() => {
    invalidateRecentSearchesSnapshot();
  });

  it("trims, deduplicates case-insensitively, and keeps most recent first", () => {
    recordRecentSearch("  Alpha  ");
    recordRecentSearch("Beta");
    recordRecentSearch("alpha");
    recordRecentSearch("   ");

    expect(getRecentSearchesSnapshot()).toEqual(["alpha", "Beta"]);
  });

  it("caps the stored list at ten searches", () => {
    for (let index = 1; index <= 12; index += 1) {
      recordRecentSearch(`query ${index}`);
    }

    expect(getRecentSearchesSnapshot()).toEqual([
      "query 12",
      "query 11",
      "query 10",
      "query 9",
      "query 8",
      "query 7",
      "query 6",
      "query 5",
      "query 4",
      "query 3",
    ]);
  });

  it("returns an empty list for malformed or unsupported storage", () => {
    const invalidValues = [
      "{",
      JSON.stringify({ items: [], version: 2 }),
      JSON.stringify({ items: {}, version: 1 }),
    ];

    for (const invalidValue of invalidValues) {
      setStoredValue(invalidValue);

      expect(() => getRecentSearchesSnapshot()).not.toThrow();
      expect(getRecentSearchesSnapshot()).toEqual([]);
    }
  });

  it("drops invalid, empty, and duplicate stored items", () => {
    setStoredValue(
      JSON.stringify({
        items: ["  Alpha  ", 42, "", "alpha", "Beta", ...Array<string>(12).fill("extra")],
        version: 1,
      }),
    );

    expect(getRecentSearchesSnapshot()).toEqual(["Alpha", "Beta", "extra"]);
  });

  it("clears searches using the versioned payload", () => {
    recordRecentSearch("Alpha");
    clearRecentSearches();

    expect(getRecentSearchesSnapshot()).toEqual([]);
    expect(window.localStorage.getItem(RECENT_SEARCHES_STORAGE_KEY)).toBe(
      JSON.stringify({ items: [], version: 1 }),
    );
  });

  it("returns a stable snapshot reference until storage changes", () => {
    const firstSnapshot = getRecentSearchesSnapshot();

    expect(getRecentSearchesSnapshot()).toBe(firstSnapshot);

    setStoredValue(JSON.stringify({ items: ["changed"], version: 1 }));

    expect(getRecentSearchesSnapshot()).not.toBe(firstSnapshot);
  });
});
