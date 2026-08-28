import { describe, expect, it } from "vitest";

import { formatRelativeTime } from "./relativeTime";

describe("formatRelativeTime", () => {
  it("formats past timestamps relative to a fixed date", () => {
    expect(
      formatRelativeTime("2026-07-03T11:00:00.000Z", new Date("2026-07-03T12:00:00.000Z"), "en"),
    ).toBe("1 hour ago");
  });

  it("returns the input when the timestamp cannot be parsed", () => {
    expect(formatRelativeTime("not-a-date", new Date("2026-07-03T12:00:00.000Z"), "en")).toBe(
      "not-a-date",
    );
  });
});
