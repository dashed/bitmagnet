import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import { initReactI18next } from "react-i18next";

export const LANGUAGE_STORAGE_KEY = "bitmagnet-language";

export const SUPPORTED_LANGUAGES = [
  {
    label: "English",
    value: "en",
  },
] as const;

type SupportedLanguage = (typeof SUPPORTED_LANGUAGES)[number]["value"];
type LocaleModule = {
  default: Record<string, unknown>;
};

const en = {
  app: {
    title: "bitmagnet",
    version: "v0.0.0",
  },
  dashboard: {
    body: "No dashboard data yet.",
    title: "Dashboard",
  },
  contentTypes: {
    audiobook: "Audiobook",
    comic: "Comic",
    ebook: "Ebook",
    game: "Game",
    movie: "Movie",
    music: "Music",
    software: "Software",
    tv_show: "TV show",
    unknown: "Unknown",
    xxx: "XXX",
  },
  detail: {
    content: "Content",
    copyInfoHash: "Copy hash",
    dhtFirstSeen: "DHT first seen",
    dhtLastSeen: "DHT last seen",
    dhtSeen: "DHT seen",
    dhtSeenCount: "DHT crawl count",
    dhtSeenSummary: "seen {{time}} · {{seenCount}}×",
    episodes: "Episodes",
    externalLinks: "External links",
    fileIndexValue: "#{{index}}",
    fileSize: "Size",
    fileType: "Type",
    files: "Files",
    filesCount: "{{count}} file",
    filesCount_other: "{{count}} files",
    filesEmpty: "No file rows are available.",
    filesLoading: "Loading files",
    filesNoInfo: "No file information is available for this torrent.",
    filesPage: "Page {{page}} of {{totalPages}}",
    filesShowingCount: "Showing {{shown}} of {{total}} files",
    firstSeen: "First seen",
    genres: "Genres",
    infoHash: "Info hash",
    languages: "Languages",
    lastSeen: "Last seen",
    loading: "Loading torrent details",
    notFoundBody: "No torrent matched this info hash.",
    notFoundTitle: "Torrent not found",
    originalMarker: "(original)",
    originalTitle: "Original title",
    overview: "Overview",
    peers: "Seeders / Leechers",
    posterAlt: "Poster for {{title}}",
    published: "Published",
    rating: "Rating",
    ratingVotes: "{{count}} vote",
    ratingVotes_other: "{{count}} votes",
    releaseDate: "Release date",
    returnToSearch: "Return to torrents",
    seen: "Seen",
    size: "Size",
    sourceSeenCount: "{{count}} time",
    sourceSeenCount_other: "{{count}} times",
    sources: "Sources",
    unknown: "Unknown",
  },
  error: {
    empty: "Nothing to show.",
    loading: "Loading...",
    notFound: "Not found",
    retry: "Retry",
    title: "Something went wrong",
  },
  fileTypes: {
    archive: "Archive",
    audio: "Audio",
    data: "Data",
    document: "Document",
    image: "Image",
    software: "Software",
    subtitles: "Subtitles",
    unknown: "Unknown",
    video: "Video",
  },
  language: {
    label: "Language",
  },
  nav: {
    classicUi: "Classic UI",
    dashboard: "Dashboard",
    torrents: "Torrents",
  },
  search: {
    apply: "Apply",
    ascending: "Ascending",
    browseEyebrow: "Newest torrents",
    clear: "Clear",
    contentType: "Content type",
    contentTypeAll: "All",
    copyMagnet: "Copy",
    copyMagnetLink: "Copy magnet link for {{title}}",
    descending: "Descending",
    dhtFirstSeen: "DHT first seen",
    dhtLastSeen: "DHT last seen",
    dhtSeen: "DHT seen",
    dhtSeenCount: "DHT crawl count",
    dhtSeenSummary: "seen {{time}} · {{seenCount}}×",
    emptyBody: "No torrents to show.",
    emptyTitle: "No torrents yet",
    filtersSummary: "Filters",
    inputLabel: "Search torrents",
    leechers: "Leechers",
    loading: "Loading search results",
    magnet: "Magnet",
    maxSize: "Max size",
    maxSizeUnit: "Max unit",
    minSize: "Min size",
    minSizeUnit: "Min unit",
    nextPage: "Next",
    noResultsBody: "Try another query.",
    noResultsTitle: "No matching torrents",
    openMagnetLink: "Open magnet link for {{title}}",
    orderBy: "Order by",
    ordering: {
      files_count: "Files count",
      info_hash: "Info hash",
      leechers: "Leechers",
      name: "Name",
      published_at: "Published at",
      relevance: "Relevance",
      seeders: "Seeders",
      size: "Size",
      updated_at: "Updated at",
    },
    page: "Page {{page}}",
    appLoadedIn: "app loaded in {{ms}} ms",
    fetchedIn: "fetched in {{ms}} ms",
    files: "Files",
    infoHash: "Info hash",
    peers: "Seeders / Leechers",
    placeholder: "Search torrents by name or hash",
    previousPage: "Previous",
    published: "Published",
    publishedAny: "Any time",
    publishedFilter: "Published date",
    publishedLastDay: "Last day",
    publishedLastMonth: "Last month",
    publishedLastThreeMonths: "Last 3 months",
    publishedLastWeek: "Last week",
    publishedLastYear: "Last year",
    refresh: "Refresh",
    resultsCount: "{{count}} result",
    resultsCount_other: "{{count}} results",
    resultsCountEstimate: "About {{count}} result",
    resultsCountEstimate_other: "About {{count}} results",
    seeders: "Seeders",
    size: "Size",
    sizeFilter: "Size",
    sizeUnits: {
      GB: "GB",
      GiB: "GiB",
      KB: "KB",
      KiB: "KiB",
      MB: "MB",
      MiB: "MiB",
      TB: "TB",
      TiB: "TiB",
    },
    sort: "Sort",
    submit: "Search",
    toggleSortDirection: "Toggle sort direction",
  },
  theme: {
    switchToDark: "Switch to dark theme",
    switchToLight: "Switch to light theme",
  },
  toast: {
    dismiss: "Dismiss notification",
    infoHashCopied: "Info hash copied",
    infoHashCopyFailed: "Could not copy info hash",
    hashCopied: "Info hash copied",
    hashCopyFailed: "Could not copy the info hash",
    magnetCopied: "Magnet link copied",
    magnetCopyFailed: "Could not copy magnet link",
    searchSubmitted: "Search submitted",
  },
};

const localeModules = import.meta.glob<LocaleModule>("./locales/*.json");

function normalizeLanguage(language: string) {
  return language.toLowerCase().split("-")[0] ?? "en";
}

export async function loadLanguage(language: string) {
  const normalized = normalizeLanguage(language);

  if (i18n.hasResourceBundle(normalized, "translation")) {
    return normalized;
  }

  const moduleLoader = localeModules[`./locales/${normalized}.json`];

  if (!moduleLoader) {
    return "en";
  }

  const loaded = await moduleLoader();
  i18n.addResourceBundle(normalized, "translation", loaded.default, true, true);

  return normalized;
}

export async function setLanguage(language: SupportedLanguage) {
  const loadedLanguage = await loadLanguage(language);
  window.localStorage.setItem(LANGUAGE_STORAGE_KEY, loadedLanguage);
  await i18n.changeLanguage(loadedLanguage);
}

void i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    detection: {
      caches: ["localStorage"],
      lookupLocalStorage: LANGUAGE_STORAGE_KEY,
      order: ["localStorage", "navigator", "htmlTag"],
    },
    fallbackLng: "en",
    interpolation: {
      escapeValue: false,
    },
    resources: {
      en: {
        translation: en,
      },
    },
    supportedLngs: SUPPORTED_LANGUAGES.map((language) => language.value),
  });

export { i18n };
