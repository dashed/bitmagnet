const WORD_CHARACTER_PATTERN = /[a-z0-9]/i;

function isWordStart(value: string, index: number) {
  if (index === 0) {
    return true;
  }

  const previous = value[index - 1];
  const current = value[index];

  if (!previous || !current) {
    return true;
  }

  if (!WORD_CHARACTER_PATTERN.test(previous)) {
    return true;
  }

  return previous === previous.toLowerCase() && current === current.toUpperCase();
}

export function fuzzyMatch(query: string, candidate: string) {
  const needle = query.trim().toLowerCase();

  if (!needle) {
    return 0;
  }

  const haystack = candidate.toLowerCase();
  let needleIndex = 0;
  let previousMatchIndex = -2;
  let consecutiveRun = 0;
  let score = 0;

  for (let index = 0; index < haystack.length; index += 1) {
    if (haystack[index] !== needle[needleIndex]) {
      continue;
    }

    score += 1;

    if (index === previousMatchIndex + 1) {
      consecutiveRun += 1;
      score += consecutiveRun * 2;
    } else {
      consecutiveRun = 0;
    }

    if (isWordStart(candidate, index)) {
      score += 3;
    }

    previousMatchIndex = index;
    needleIndex += 1;

    if (needleIndex === needle.length) {
      return score;
    }
  }

  return null;
}
