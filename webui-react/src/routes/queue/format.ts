import { formatRelativeTime } from "../../utils/relativeTime";

const DATE_TIME_FORMATTERS = new Map<string, Intl.DateTimeFormat>();

function getDateTimeFormatter(locale: string) {
  const cachedFormatter = DATE_TIME_FORMATTERS.get(locale);

  if (cachedFormatter) {
    return cachedFormatter;
  }

  const formatter = new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  });
  DATE_TIME_FORMATTERS.set(locale, formatter);

  return formatter;
}

export function formatQueueDateTime(value: string | null | undefined, locale: string) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.valueOf())) {
    return value;
  }

  return getDateTimeFormatter(locale).format(date);
}

export function formatQueueRelativeTime(
  value: string | null | undefined,
  now: Date,
  locale: string,
) {
  if (!value) {
    return "";
  }

  return formatRelativeTime(value, now, locale);
}

export function prettifyQueuePayload(payload: string) {
  try {
    return JSON.stringify(JSON.parse(payload) as unknown, null, 2) ?? payload;
  } catch {
    return payload;
  }
}

export function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "Unknown error";
}
