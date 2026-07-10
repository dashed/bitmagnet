import { describe, expect, it } from "vitest";

import { filterCommands } from "./filterCommands";
import { type Command, type CommandGroupId } from "./types";

function createCommand(group: CommandGroupId, id: string, title = id): Command {
  return {
    group,
    id,
    perform: () => undefined,
    title,
  };
}

describe("filterCommands", () => {
  it("returns an empty query in stable group order", () => {
    const commands = [
      createCommand("language", "language"),
      createCommand("actions", "action"),
      createCommand("navigation", "navigation-first"),
      createCommand("theme", "theme"),
      createCommand("navigation", "navigation-second"),
      createCommand("recent", "recent"),
      createCommand("saved", "saved"),
      createCommand("search", "search"),
    ];

    expect(filterCommands(commands, "").map((command) => command.id)).toEqual([
      "navigation-first",
      "navigation-second",
      "search",
      "action",
      "saved",
      "recent",
      "theme",
      "language",
    ]);
  });

  it("filters non-matches and ranks stronger fuzzy matches first", () => {
    const commands = [
      createCommand("navigation", "data-shell", "Data shell"),
      createCommand("navigation", "queue", "Queue"),
      createCommand("navigation", "dashboard", "Dashboard"),
    ];

    expect(filterCommands(commands, "dash").map((command) => command.id)).toEqual([
      "dashboard",
      "data-shell",
    ]);
  });

  it("keeps the search command pinned ahead of ranked results", () => {
    const commands = [
      createCommand("navigation", "dashboard", "Dashboard"),
      createCommand("search", "search-query", "Search torrents for matrix"),
      createCommand("navigation", "queue", "Queue"),
    ];

    expect(filterCommands(commands, "dash").map((command) => command.id)).toEqual([
      "search-query",
      "dashboard",
    ]);
  });
});
