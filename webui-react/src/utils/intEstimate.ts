const NumberFormatter = Intl.NumberFormat;
const NUMBER_FORMATTERS = new Map<string, Intl.NumberFormat>();

function getNumberFormatter(locale: string) {
  const cachedFormatter = NUMBER_FORMATTERS.get(locale);

  if (cachedFormatter) {
    return cachedFormatter;
  }

  const formatter = new NumberFormatter(locale);
  NUMBER_FORMATTERS.set(locale, formatter);

  return formatter;
}

export function formatIntEstimate(value: number, isEstimate = true, sigFigs = 2, locale = "en") {
  let nextValue = value;

  if (isEstimate && nextValue > 0 && sigFigs > 0) {
    const magnitude = Math.floor(Math.log10(Math.abs(nextValue)));
    const scale = Math.pow(10, magnitude - (sigFigs - 1));
    nextValue = Math.round(nextValue / scale) * scale;
  }

  const formatted = getNumberFormatter(locale).format(nextValue);

  return isEstimate ? `~${formatted}` : formatted;
}
