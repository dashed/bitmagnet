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
  error: {
    empty: "Nothing to show.",
    loading: "Loading...",
    retry: "Retry",
    title: "Something went wrong",
  },
  language: {
    label: "Language",
  },
  nav: {
    dashboard: "Dashboard",
    torrents: "Torrents",
  },
  search: {
    emptyBody: "No torrents to show.",
    emptyTitle: "Start with a torrent search",
    inputLabel: "Search torrents",
    placeholder: "Search torrents by name or hash",
    submit: "Search",
  },
  theme: {
    switchToDark: "Switch to dark theme",
    switchToLight: "Switch to light theme",
  },
  toast: {
    dismiss: "Dismiss notification",
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
