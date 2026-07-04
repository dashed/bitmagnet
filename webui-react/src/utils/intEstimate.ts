export function formatIntEstimate(value: number, isEstimate = true, sigFigs = 2, locale = "en") {
  let nextValue = value;

  if (isEstimate && nextValue > 0 && sigFigs > 0) {
    const magnitude = Math.floor(Math.log10(Math.abs(nextValue)));
    const scale = Math.pow(10, magnitude - (sigFigs - 1));
    nextValue = Math.round(nextValue / scale) * scale;
  }

  const formatted = new Intl.NumberFormat(locale).format(nextValue);

  return isEstimate ? `~${formatted}` : formatted;
}
