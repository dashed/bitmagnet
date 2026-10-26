const INNER_SEPARATOR_PATTERN = /([\p{L}\p{N}])[._-]+(?=[\p{L}\p{N}])/gu;
const STRAY_SYMBOL_PATTERN = /[^\p{L}\p{N}\s]/gu;
const WHITESPACE_PATTERN = /\s+/gu;

function stripSurroundingQuotes(value: string) {
  const firstCharacter = value[0];
  const lastCharacter = value[value.length - 1];
  const hasSurroundingQuotes =
    (firstCharacter === '"' && lastCharacter === '"') ||
    (firstCharacter === "'" && lastCharacter === "'") ||
    (firstCharacter === "“" && lastCharacter === "”") ||
    (firstCharacter === "‘" && lastCharacter === "’");

  return hasSurroundingQuotes ? value.slice(1, -1).trim() : value;
}

export function simplifyQuery(query: string): string {
  const trimmedQuery = query.trim();

  if (!trimmedQuery) {
    return "";
  }

  // Recovery retries stay unquoted: quoting the whole suggestion enforces a
  // phrase and narrows matching, while separate cleaned terms broaden it.
  const simplifiedQuery = stripSurroundingQuotes(trimmedQuery)
    .replace(INNER_SEPARATOR_PATTERN, "$1 ")
    .replace(STRAY_SYMBOL_PATTERN, "")
    .replace(WHITESPACE_PATTERN, " ")
    .trim();

  return simplifiedQuery || trimmedQuery;
}
