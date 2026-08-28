import { describe, expect, it } from "vitest";

import { formatIntEstimate } from "./intEstimate";

describe("formatIntEstimate", () => {
  it("rounds estimates to significant figures and preserves exact counts", () => {
    expect(formatIntEstimate(12_345, true, 2, "en")).toBe("~12,000");
    expect(formatIntEstimate(12_345, false, 2, "en")).toBe("12,345");
    expect(formatIntEstimate(0, true, 2, "en")).toBe("~0");
  });
});
