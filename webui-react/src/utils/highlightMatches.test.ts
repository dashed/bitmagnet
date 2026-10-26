import { describe, expect, it } from "vitest";

import { highlightMatches } from "./highlightMatches";

describe("highlightMatches", () => {
  it("returns ordered case-insensitive match segments", () => {
    expect(highlightMatches("The Matrix Returns", "matrix returns")).toEqual([
      { match: false, text: "The " },
      { match: true, text: "Matrix" },
      { match: false, text: " " },
      { match: true, text: "Returns" },
    ]);
  });

  it("escapes regular-expression characters in query terms", () => {
    expect(highlightMatches("Use a+b and [test].", "a+b [test]")).toEqual([
      { match: false, text: "Use " },
      { match: true, text: "a+b" },
      { match: false, text: " and " },
      { match: true, text: "[test]" },
      { match: false, text: "." },
    ]);
  });

  it("returns one non-match segment for an empty query", () => {
    expect(highlightMatches("Unchanged", "")).toEqual([{ match: false, text: "Unchanged" }]);
  });

  it("merges overlapping and adjacent matches", () => {
    expect(highlightMatches("foobar ababa", "foo bar aba bab")).toEqual([
      { match: true, text: "foobar" },
      { match: false, text: " " },
      { match: true, text: "ababa" },
    ]);
  });

  it("ignores one-character terms", () => {
    expect(highlightMatches("a 😀 bc", "a 😀 bc")).toEqual([
      { match: false, text: "a 😀 " },
      { match: true, text: "bc" },
    ]);
  });
});
