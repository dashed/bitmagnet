import { describe, expect, it } from "vitest";

import { compareFileRows, compareFileRowsByPath, type SortableFileRow } from "./torrentFileSort";

const rows: SortableFileRow[] = [
  { fileType: "video", index: 2, path: "Zoo/movie.mkv", size: 300 },
  { fileType: "audio", index: 1, path: "alpha/song.flac", size: 100 },
  { fileType: "archive", index: 10, path: "Beta/data.zip", size: 200 },
];

describe("file row comparators", () => {
  it("sorts by index numerically", () => {
    expect(
      [...rows]
        .sort((left, right) => compareFileRows(left, right, { direction: "asc", field: "index" }))
        .map((row) => row.index),
    ).toEqual([1, 2, 10]);
  });

  it("sorts by path case-insensitively", () => {
    expect([...rows].sort(compareFileRowsByPath).map((row) => row.path)).toEqual([
      "alpha/song.flac",
      "Beta/data.zip",
      "Zoo/movie.mkv",
    ]);
  });

  it("sorts by type as strings", () => {
    expect(
      [...rows]
        .sort((left, right) => compareFileRows(left, right, { direction: "asc", field: "type" }))
        .map((row) => row.fileType),
    ).toEqual(["archive", "audio", "video"]);
  });

  it("sorts by size numerically", () => {
    expect(
      [...rows]
        .sort((left, right) => compareFileRows(left, right, { direction: "desc", field: "size" }))
        .map((row) => row.size),
    ).toEqual([300, 200, 100]);
  });
});
