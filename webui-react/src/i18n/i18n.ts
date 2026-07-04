import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import { initReactI18next } from "react-i18next";

import en from "./locales/en";

export const LANGUAGE_STORAGE_KEY = "bitmagnet-lng";

export const SUPPORTED_LANGUAGES = [
  {
    label: "العربية",
    value: "ar",
  },
  {
    label: "Català",
    value: "ca",
  },
  {
    label: "Deutsch",
    value: "de",
  },
  {
    label: "English",
    value: "en",
  },
  {
    label: "Español",
    value: "es",
  },
  {
    label: "Français",
    value: "fr",
  },
  {
    label: "हिन्दी",
    value: "hi",
  },
  {
    label: "日本語",
    value: "ja",
  },
  {
    label: "Nederlands",
    value: "nl",
  },
  {
    label: "Português",
    value: "pt",
  },
  {
    label: "Русский",
    value: "ru",
  },
  {
    label: "Türkçe",
    value: "tr",
  },
  {
    label: "Українська",
    value: "uk",
  },
  {
    label: "中文",
    value: "zh",
  },
] as const;

export type SupportedLanguage = (typeof SUPPORTED_LANGUAGES)[number]["value"];

type LocaleResource = Record<string, unknown>;
type LocaleModule = {
  default: LocaleResource;
};

const SUPPORTED_LANGUAGE_VALUES = SUPPORTED_LANGUAGES.map((language) => language.value);
const SUPPORTED_LANGUAGE_SET = new Set<string>(SUPPORTED_LANGUAGE_VALUES);
const RTL_LANGUAGES = new Set<SupportedLanguage>(["ar"]);

const localeLoaders = {
  ar: () => import("./locales/ar"),
  ca: () => import("./locales/ca"),
  de: () => import("./locales/de"),
  es: () => import("./locales/es"),
  fr: () => import("./locales/fr"),
  hi: () => import("./locales/hi"),
  ja: () => import("./locales/ja"),
  nl: () => import("./locales/nl"),
  pt: () => import("./locales/pt"),
  ru: () => import("./locales/ru"),
  tr: () => import("./locales/tr"),
  uk: () => import("./locales/uk"),
  zh: () => import("./locales/zh"),
} satisfies Partial<Record<SupportedLanguage, () => Promise<LocaleModule>>>;

const pendingLanguageLoads = new Map<SupportedLanguage, Promise<SupportedLanguage>>();

export function normalizeLanguage(language: string | null | undefined): SupportedLanguage {
  const normalized = language?.trim().toLowerCase().replace("_", "-");
  const baseLanguage = normalized?.split("-")[0];

  return baseLanguage && SUPPORTED_LANGUAGE_SET.has(baseLanguage)
    ? (baseLanguage as SupportedLanguage)
    : "en";
}

export function getLanguageDirection(language: string | null | undefined) {
  return RTL_LANGUAGES.has(normalizeLanguage(language)) ? "rtl" : "ltr";
}

export function applyDocumentLanguage(language: string | null | undefined) {
  if (typeof document === "undefined") {
    return;
  }

  const normalized = normalizeLanguage(language);
  document.documentElement.lang = normalized;
  document.documentElement.dir = getLanguageDirection(normalized);
}

function persistLanguage(language: SupportedLanguage) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(LANGUAGE_STORAGE_KEY, language);
  } catch {
    // localStorage can be disabled; language selection still works for the session.
  }
}

export async function loadLanguage(language: string | null | undefined) {
  const normalized = normalizeLanguage(language);

  if (normalized === "en" || i18n.hasResourceBundle(normalized, "translation")) {
    return normalized;
  }

  const pendingLoad = pendingLanguageLoads.get(normalized);
  if (pendingLoad) {
    return pendingLoad;
  }

  const moduleLoader = localeLoaders[normalized];
  if (!moduleLoader) {
    return "en";
  }

  const loadPromise = moduleLoader()
    .then((loaded) => {
      i18n.addResourceBundle(normalized, "translation", loaded.default, true, true);
      return normalized;
    })
    .finally(() => {
      pendingLanguageLoads.delete(normalized);
    });

  pendingLanguageLoads.set(normalized, loadPromise);

  return loadPromise;
}

export async function setLanguage(language: string) {
  const loadedLanguage = await loadLanguage(language);
  persistLanguage(loadedLanguage);
  await i18n.changeLanguage(loadedLanguage);
}

function handleLanguageChanged(language: string) {
  const normalized = normalizeLanguage(language);
  applyDocumentLanguage(normalized);

  if (normalized === "en" || i18n.hasResourceBundle(normalized, "translation")) {
    return;
  }

  void loadLanguage(normalized).then((loadedLanguage) => {
    if (loadedLanguage === normalizeLanguage(i18n.language)) {
      void i18n.changeLanguage(loadedLanguage);
    }
  });
}

i18n.on("languageChanged", handleLanguageChanged);

void i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    detection: {
      caches: ["localStorage"],
      convertDetectedLanguage: normalizeLanguage,
      lookupLocalStorage: LANGUAGE_STORAGE_KEY,
      order: ["localStorage", "navigator", "htmlTag"],
    },
    fallbackLng: "en",
    interpolation: {
      escapeValue: false,
    },
    load: "languageOnly",
    nonExplicitSupportedLngs: true,
    partialBundledLanguages: true,
    react: {
      bindI18nStore: "added",
    },
    resources: {
      en: {
        translation: en,
      },
    },
    returnEmptyString: false,
    supportedLngs: SUPPORTED_LANGUAGE_VALUES,
  })
  .then(() => {
    handleLanguageChanged(i18n.resolvedLanguage ?? i18n.language);
  });

export { i18n };
