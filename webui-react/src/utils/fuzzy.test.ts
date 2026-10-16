import { describe, expect, it } from "vitest";

import { fuzzyMatch } from "./fuzzy";

describe("fuzzyMatch", () => {
  it("matches case-insensitive subsequences", () => {
    expect(fuzzyMatch("FBR", "foo/bar")).not.toBeNull();
  });

  it("scores word-start matches higher", () => {
    const wordStartScore = fuzzyMatch("fb", "foo/bar");
    const midWordScore = fuzzyMatch("fb", "foob/ar");

    expect(wordStartScore).not.toBeNull();
    expect(midWordScore).not.toBeNull();
    expect(wordStartScore).toBeGreaterThan(midWordScore ?? 0);
  });

  it("returns null for non-matches", () => {
    expect(fuzzyMatch("zzz", "foo/bar")).toBeNull();
  });
});
