import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  LANGUAGE_STORAGE_KEY,
  SUPPORTED_LANGUAGES,
  getLanguageDirection,
  i18n,
  normalizeLanguage,
  setLanguage,
} from "./i18n";
import ca from "./locales/ca";
import en from "./locales/en";

function getCatalogValue(catalog: unknown, key: string) {
  return key.split(".").reduce<unknown>((current, part) => {
    if (!current || typeof current !== "object" || !(part in current)) {
      return undefined;
    }

    return (current as Record<string, unknown>)[part];
  }, catalog);
}

function flattenCatalogValues(catalog: unknown): string[] {
  if (!catalog || typeof catalog !== "object") {
    return [String(catalog)];
  }

  return Object.values(catalog).flatMap((value) => flattenCatalogValues(value));
}

function expectCatalogKeys(prefix: string, keys: readonly string[]) {
  for (const key of keys) {
    expect(getCatalogValue(en, `${prefix}.${key}`), `${prefix}.${key}`).toEqual(expect.any(String));
  }
}

async function waitForI18n() {
  if (i18n.isInitialized) {
    return;
  }

  await new Promise<void>((resolve) => {
    i18n.on("initialized", () => resolve());
  });
}

describe("i18n infrastructure", () => {
  beforeEach(async () => {
    window.localStorage.clear();
    await waitForI18n();
  });

  afterEach(async () => {
    await setLanguage("en");
    window.localStorage.clear();
  });

  it("lists the Angular language set by native name", () => {
    expect(SUPPORTED_LANGUAGES).toEqual([
      { label: "العربية", value: "ar" },
      { label: "Català", value: "ca" },
      { label: "Deutsch", value: "de" },
      { label: "English", value: "en" },
      { label: "Español", value: "es" },
      { label: "Français", value: "fr" },
      { label: "हिन्दी", value: "hi" },
      { label: "日本語", value: "ja" },
      { label: "Nederlands", value: "nl" },
      { label: "Português", value: "pt" },
      { label: "Русский", value: "ru" },
      { label: "Türkçe", value: "tr" },
      { label: "Українська", value: "uk" },
      { label: "中文", value: "zh" },
    ]);
  });

  it("strips regional tags to shipped base languages", () => {
    expect(normalizeLanguage("pt-BR")).toBe("pt");
    expect(normalizeLanguage("en-US")).toBe("en");
    expect(normalizeLanguage("zh-Hans")).toBe("zh");
    expect(normalizeLanguage("not-a-language")).toBe("en");
  });

  it("persists language changes and updates document lang and dir", async () => {
    await setLanguage("ar");

    expect(window.localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe("ar");
    expect(document.documentElement.lang).toBe("ar");
    expect(document.documentElement.dir).toBe("rtl");
    expect(getLanguageDirection("ar")).toBe("rtl");

    await setLanguage("pt-BR");

    expect(window.localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe("pt");
    expect(document.documentElement.lang).toBe("pt");
    expect(document.documentElement.dir).toBe("ltr");
  });

  it("strips fallback markers and incompatible placeholders from lazy catalogs", async () => {
    expect(flattenCatalogValues(ca)).not.toContain("__missing__");
    expect(getCatalogValue(ca, "detail.filesNoInfo")).toBeUndefined();

    await setLanguage("ca");

    expect(i18n.t("detail.filesNoInfo")).toBe(en.detail.filesNoInfo);
  });

  it("keeps finite dynamic-key families in the English catalog", () => {
    expectCatalogKeys("actions.tags", ["deleteSuccess", "putSuccess", "setSuccess"]);
    expectCatalogKeys("contentTypes", [
      "audiobook",
      "comic",
      "ebook",
      "game",
      "movie",
      "music",
      "software",
      "tv_show",
      "unknown",
      "xxx",
    ]);
    expectCatalogKeys("contentTypesPlural", [
      "audiobook",
      "comic",
      "ebook",
      "game",
      "movie",
      "music",
      "software",
      "tv_show",
      "unknown",
      "xxx",
    ]);
    expectCatalogKeys("dash.health.status", ["down", "inactive", "unknown", "up"]);
    expectCatalogKeys("dash.queue.status", ["failed", "pending", "processed", "retry"]);
    expectCatalogKeys("facets", [
      "file_type",
      "genre",
      "language",
      "torrent_source",
      "torrent_tag",
      "video_resolution",
      "video_source",
    ]);
    expectCatalogKeys("fileTypes", [
      "archive",
      "audio",
      "data",
      "document",
      "image",
      "software",
      "subtitles",
      "unknown",
      "video",
    ]);
    expectCatalogKeys("health.overallStatuses", ["degraded", "ok"]);
    expectCatalogKeys("health.statuses", ["down", "inactive", "unknown", "up"]);
    expectCatalogKeys("health.workerStates", ["started", "stopped"]);
    expectCatalogKeys("metrics.autoRefresh", [
      "minutes_1",
      "minutes_5",
      "off",
      "seconds_10",
      "seconds_30",
    ]);
    expectCatalogKeys("metrics.bucketDurations", ["day", "hour", "minute"]);
    expectCatalogKeys("metrics.events", ["created", "failed", "processed", "updated"]);
    expectCatalogKeys("metrics.statuses", ["failed", "pending", "processed", "retry"]);
    expectCatalogKeys("metrics.timeframes", [
      "all",
      "days_1",
      "hours_1",
      "hours_6",
      "hours_12",
      "minutes_15",
      "minutes_30",
      "weeks_1",
    ]);
    expectCatalogKeys("palette.groups", [
      "actions",
      "language",
      "navigation",
      "saved",
      "search",
      "theme",
    ]);
    expectCatalogKeys("queue.order", ["created_at", "priority", "ran_at"]);
    expectCatalogKeys("queue.status", ["failed", "pending", "processed", "retry"]);
    expectCatalogKeys("search.ordering", [
      "files_count",
      "info_hash",
      "last_seen",
      "leechers",
      "name",
      "path",
      "published_at",
      "relevance",
      "seeders",
      "size",
      "updated_at",
    ]);
    expectCatalogKeys("search.sizeUnits", ["GB", "GiB", "KB", "KiB", "MB", "MiB", "TB", "TiB"]);
  });
});
