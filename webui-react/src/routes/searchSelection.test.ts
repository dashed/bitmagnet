import { describe, expect, it } from "vitest";

import {
  clearSelectionOnSearchParamsChange,
  getPageSelectionState,
  toggleInfoHashSelection,
  togglePageSelection,
} from "./searchSelection";

describe("search result selection", () => {
  it("adds and removes selected info hashes", () => {
    const selected = toggleInfoHashSelection(new Set<string>(), "hash-a", true);

    expect(Array.from(selected)).toEqual(["hash-a"]);
    expect(Array.from(toggleInfoHashSelection(selected, "hash-a", false))).toEqual([]);
  });

  it("selects and clears the current page", () => {
    const pageInfoHashes = ["hash-a", "hash-b"];
    const selected = togglePageSelection(new Set<string>(["hash-c"]), pageInfoHashes);

    expect(Array.from(selected).sort()).toEqual(["hash-a", "hash-b", "hash-c"]);
    expect(getPageSelectionState(selected, pageInfoHashes)).toEqual({
      allSelected: true,
      partiallySelected: false,
      selectedOnPage: 2,
    });

    const clearedPage = togglePageSelection(selected, pageInfoHashes);

    expect(Array.from(clearedPage)).toEqual(["hash-c"]);
  });

  it("clears selection when search params change", () => {
    const selected = new Set(["hash-a"]);

    expect(clearSelectionOnSearchParamsChange(selected, "query=matrix", "query=matrix")).toBe(
      selected,
    );
    expect(
      Array.from(clearSelectionOnSearchParamsChange(selected, "query=matrix", "query=alien")),
    ).toEqual([]);
  });
});
