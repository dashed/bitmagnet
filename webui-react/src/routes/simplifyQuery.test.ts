import { describe, expect, it } from "vitest";

import { simplifyQuery } from "./simplifyQuery";

describe("simplifyQuery", () => {
  it("trims and collapses whitespace", () => {
    expect(simplifyQuery("  one\n\t two   three  ")).toBe("one two three");
  });

  it("strips matching surrounding quotes", () => {
    expect(simplifyQuery('  "quoted search"  ')).toBe("quoted search");
    expect(simplifyQuery("‘curly quote’")).toBe("curly quote");
  });

  it("turns noisy token separators into spaces and drops stray symbols", () => {
    expect(simplifyQuery('"The.Matrix-Reloaded_1080p!!!"')).toBe(
      "The Matrix Reloaded 1080p",
    );
  });

  it("falls back to the trimmed query when simplification removes everything", () => {
    expect(simplifyQuery("  !!!  ")).toBe("!!!");
  });

  it("returns an empty string for an empty query", () => {
    expect(simplifyQuery("   ")).toBe("");
  });
});
