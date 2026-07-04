import { describe, expect, it } from "vitest";

import {
  DEFAULT_REPROCESS_OPTIONS,
  addTagName,
  canConfirmDelete,
  canSubmitTagMutation,
  getNextReprocessOptions,
  getNextSuggestionIndex,
  getSubmittedTags,
  normalizeTagName,
  removeTagName,
} from "./torrentMutationActions";

describe("torrent mutation action helpers", () => {
  it("normalizes chip input and keeps tag names unique", () => {
    const withTag = addTagName([], " movie ");

    expect(withTag).toEqual(["movie"]);
    expect(addTagName(withTag, "movie")).toEqual(["movie"]);
    expect(removeTagName(withTag, "movie")).toEqual([]);
    expect(getSubmittedTags(["movie"], "  tv ")).toEqual(["movie", "tv"]);
  });

  it("normalizes tag names with kebab-case slug rules", () => {
    expect(normalizeTagName("Action Movie")).toBe("action-movie");
    expect(normalizeTagName("Sci-Fi!")).toBe("sci-fi");
    expect(normalizeTagName("---Action---Movie")).toBe("action-movie");
    expect(normalizeTagName("already-kebab")).toBe("already-kebab");
  });

  it("selects tag suggestions with keyboard wraparound", () => {
    expect(getNextSuggestionIndex(-1, 3, "down")).toBe(0);
    expect(getNextSuggestionIndex(2, 3, "down")).toBe(0);
    expect(getNextSuggestionIndex(-1, 3, "up")).toBe(2);
    expect(getNextSuggestionIndex(0, 3, "up")).toBe(2);
    expect(getNextSuggestionIndex(0, 0, "down")).toBe(-1);
  });

  it("allows replacing tags with an empty set but blocks add/remove without tags", () => {
    expect(canSubmitTagMutation("set", 2, [], "", false)).toBe(true);
    expect(canSubmitTagMutation("put", 2, [], "", false)).toBe(false);
    expect(canSubmitTagMutation("delete", 2, [], "", false)).toBe(false);
    expect(canSubmitTagMutation("put", 2, [], "movie", false)).toBe(true);
    expect(canSubmitTagMutation("set", 0, ["movie"], "", false)).toBe(false);
    expect(canSubmitTagMutation("set", 2, ["movie"], "", true)).toBe(false);
  });

  it("matches Angular reprocess defaults and checkbox coupling", () => {
    expect(DEFAULT_REPROCESS_OPTIONS).toEqual({
      apisDisabled: true,
      classifierRematch: false,
      localSearchDisabled: true,
    });

    const withApis = getNextReprocessOptions(DEFAULT_REPROCESS_OPTIONS, "apis", true);

    expect(withApis).toEqual({
      apisDisabled: false,
      classifierRematch: false,
      localSearchDisabled: false,
    });

    expect(getNextReprocessOptions(withApis, "local", false)).toEqual({
      apisDisabled: true,
      classifierRematch: false,
      localSearchDisabled: true,
    });
  });

  it("requires delete acknowledgement before confirm is enabled", () => {
    expect(canConfirmDelete(2, false, false)).toBe(false);
    expect(canConfirmDelete(2, true, false)).toBe(true);
    expect(canConfirmDelete(0, true, false)).toBe(false);
    expect(canConfirmDelete(2, true, true)).toBe(false);
  });
});
