const RELATIVE_TIME_UNITS: ReadonlyArray<readonly [Intl.RelativeTimeFormatUnit, number]> = [
  ["year", 1000 * 60 * 60 * 24 * 365],
  ["month", 1000 * 60 * 60 * 24 * 30],
  ["week", 1000 * 60 * 60 * 24 * 7],
  ["day", 1000 * 60 * 60 * 24],
  ["hour", 1000 * 60 * 60],
  ["minute", 1000 * 60],
  ["second", 1000],
];

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
  const formatter = new Intl.RelativeTimeFormat(locales, {
    numeric: "auto",
  });

  const [unit, unitMs] =
    RELATIVE_TIME_UNITS.find(([, candidateUnitMs]) => absoluteDiff >= candidateUnitMs) ??
    RELATIVE_TIME_UNITS[RELATIVE_TIME_UNITS.length - 1];

  return formatter.format(Math.round(diff / unitMs), unit);
}
