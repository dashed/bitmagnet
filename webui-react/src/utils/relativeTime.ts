const RELATIVE_TIME_UNITS: ReadonlyArray<readonly [Intl.RelativeTimeFormatUnit, number]> = [
  ["year", 1000 * 60 * 60 * 24 * 365],
  ["month", 1000 * 60 * 60 * 24 * 30],
  ["week", 1000 * 60 * 60 * 24 * 7],
  ["day", 1000 * 60 * 60 * 24],
  ["hour", 1000 * 60 * 60],
  ["minute", 1000 * 60],
  ["second", 1000],
];
const RelativeTimeFormatter = Intl.RelativeTimeFormat;
const RELATIVE_TIME_FORMATTERS = new Map<string, Intl.RelativeTimeFormat>();

function getRelativeTimeFormatter(locales?: Intl.LocalesArgument) {
  const key = Array.isArray(locales) ? locales.map(String).join("\0") : String(locales ?? "");
  const cachedFormatter = RELATIVE_TIME_FORMATTERS.get(key);

  if (cachedFormatter) {
    return cachedFormatter;
  }

  const formatter = new RelativeTimeFormatter(locales, {
    numeric: "auto",
  });
  RELATIVE_TIME_FORMATTERS.set(key, formatter);

  return formatter;
}

export function formatRelativeTime(
  value: string,
  now = new Date(),
  locales?: Intl.LocalesArgument,
) {
  const date = new Date(value);
  const diff = date.getTime() - now.getTime();

  if (!Number.isFinite(diff)) {
    return value;
  }

  const absoluteDiff = Math.abs(diff);
  const formatter = getRelativeTimeFormatter(locales);

  const [unit, unitMs] =
    RELATIVE_TIME_UNITS.find(([, candidateUnitMs]) => absoluteDiff >= candidateUnitMs) ??
    RELATIVE_TIME_UNITS[RELATIVE_TIME_UNITS.length - 1];

  return formatter.format(Math.round(diff / unitMs), unit);
}
