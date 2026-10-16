const REGEXP_ESCAPE_PATTERN = /[.*+?^${}()|[\]\\]/g;
const WHITESPACE_PATTERN = /\s+/u;

function escapeRegExp(value: string) {
  return value.replace(REGEXP_ESCAPE_PATTERN, "\\$&");
}

export function highlightMatches(
  text: string,
  query: string,
): Array<{ match: boolean; text: string }> {
  const queryTerms = new Map<string, string>();

  for (const term of query.trim().split(WHITESPACE_PATTERN)) {
    if (Array.from(term).length >= 2) {
      const normalizedTerm = term.toLowerCase();

      if (!queryTerms.has(normalizedTerm)) {
        queryTerms.set(normalizedTerm, term);
      }
    }
  }

  if (queryTerms.size === 0 || !text) {
    return [{ match: false, text }];
  }

  const matchRanges: Array<[number, number]> = [];

  for (const term of queryTerms.values()) {
    const termPattern = new RegExp(escapeRegExp(term), "giu");
    let match = termPattern.exec(text);

    while (match) {
      matchRanges.push([match.index, match.index + match[0].length]);
      termPattern.lastIndex = match.index + Array.from(match[0])[0].length;
      match = termPattern.exec(text);
    }
  }

  if (matchRanges.length === 0) {
    return [{ match: false, text }];
  }

  matchRanges.sort((left, right) => left[0] - right[0] || right[1] - left[1]);

  const mergedRanges: Array<[number, number]> = [];

  for (const [start, end] of matchRanges) {
    const previousRange = mergedRanges[mergedRanges.length - 1];

    if (previousRange && start <= previousRange[1]) {
      previousRange[1] = Math.max(previousRange[1], end);
    } else {
      mergedRanges.push([start, end]);
    }
  }

  const segments: Array<{ match: boolean; text: string }> = [];
  let cursor = 0;

  for (const [start, end] of mergedRanges) {
    if (start > cursor) {
      segments.push({ match: false, text: text.slice(cursor, start) });
    }

    segments.push({ match: true, text: text.slice(start, end) });
    cursor = end;
  }

  if (cursor < text.length) {
    segments.push({ match: false, text: text.slice(cursor) });
  }

  return segments;
}
