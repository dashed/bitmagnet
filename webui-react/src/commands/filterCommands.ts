import { fuzzyMatch } from "../utils/fuzzy";
import { type Command, type CommandGroupId } from "./types";

const GROUP_ORDER: Record<CommandGroupId, number> = {
  actions: 2,
  language: 6,
  navigation: 0,
  recent: 4,
  saved: 3,
  search: 1,
  theme: 5,
};

export function filterCommands(commands: Command[], query: string): Command[] {
  const normalizedQuery = query.trim();

  if (!normalizedQuery) {
    return commands
      .map((command, index) => ({ command, index }))
      .sort(
        (left, right) =>
          GROUP_ORDER[left.command.group] - GROUP_ORDER[right.command.group] ||
          left.index - right.index,
      )
      .map(({ command }) => command);
  }

  const pinnedSearchIndex = commands.findIndex((command) => command.group === "search");
  const pinnedSearchCommand = commands[pinnedSearchIndex];
  const scoredCommands: Array<{ command: Command; index: number; score: number }> = [];

  commands.forEach((command, index) => {
    if (index === pinnedSearchIndex) {
      return;
    }

    const score = fuzzyMatch(normalizedQuery, `${command.title} ${command.keywords ?? ""}`);

    if (score !== null) {
      scoredCommands.push({ command, index, score });
    }
  });

  const rankedCommands = scoredCommands
    .sort((left, right) => right.score - left.score || left.index - right.index)
    .map(({ command }) => command);

  return pinnedSearchCommand ? [pinnedSearchCommand, ...rankedCommands] : rankedCommands;
}
