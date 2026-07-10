import { useSyncExternalStore } from "react";

type RecentSearchesStorage = {
  items: string[];
  version: 1;
};

const EMPTY_RECENT_SEARCHES: string[] = [];
const RECENT_SEARCHES_LIMIT = 10;
const RECENT_SEARCHES_STORAGE_KEY = "bitmagnet-recent-searches";
const RECENT_SEARCHES_STORAGE_VERSION = 1;
const listeners = new Set<() => void>();

let cachedSnapshot: string[] | undefined;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const prototype: unknown = Object.getPrototypeOf(value);

  return prototype === null || prototype === Object.prototype;
}

function sanitizeRecentSearches(items: unknown[]): string[] {
  const queries: string[] = [];
  const seenQueries = new Set<string>();

  for (const item of items) {
    if (typeof item !== "string") {
      continue;
    }

    const query = item.trim();
    const normalizedQuery = query.toLowerCase();

    if (!query || seenQueries.has(normalizedQuery)) {
      continue;
    }

    queries.push(query);
    seenQueries.add(normalizedQuery);

    if (queries.length === RECENT_SEARCHES_LIMIT) {
      break;
    }
  }

  return queries;
}

function parseRecentSearches(rawValue: string | null): string[] {
  if (!rawValue) {
    return [];
  }

  try {
    const value: unknown = JSON.parse(rawValue);

    if (
      !isPlainObject(value) ||
      !Array.isArray(value["items"]) ||
      value["version"] !== RECENT_SEARCHES_STORAGE_VERSION
    ) {
      return [];
    }

    return sanitizeRecentSearches(value["items"]);
  } catch {
    return [];
  }
}

function readRecentSearches(): string[] {
  if (typeof window === "undefined") {
    return EMPTY_RECENT_SEARCHES;
  }

  try {
    return parseRecentSearches(window.localStorage.getItem(RECENT_SEARCHES_STORAGE_KEY));
  } catch {
    return [];
  }
}

function notifyRecentSearchesListeners() {
  for (const listener of listeners) {
    listener();
  }
}

function persistRecentSearches(items: string[]) {
  const storage: RecentSearchesStorage = {
    items,
    version: RECENT_SEARCHES_STORAGE_VERSION,
  };

  if (typeof window !== "undefined") {
    try {
      window.localStorage.setItem(RECENT_SEARCHES_STORAGE_KEY, JSON.stringify(storage));
    } catch {
      // Keep the in-memory snapshot usable when storage is unavailable.
    }
  }

  cachedSnapshot = items;
  notifyRecentSearchesListeners();
}

function getRecentSearchesServerSnapshot() {
  return EMPTY_RECENT_SEARCHES;
}

function handleRecentSearchesStorage(event: StorageEvent) {
  if (event.key !== null && event.key !== RECENT_SEARCHES_STORAGE_KEY) {
    return;
  }

  cachedSnapshot = undefined;
  notifyRecentSearchesListeners();
}

if (typeof window !== "undefined") {
  window.addEventListener("storage", handleRecentSearchesStorage);
}

export function clearRecentSearches() {
  persistRecentSearches([]);
}

export function getRecentSearchesSnapshot(): string[] {
  cachedSnapshot ??= readRecentSearches();

  return cachedSnapshot;
}

export function recordRecentSearch(query: string) {
  const trimmedQuery = query.trim();

  if (!trimmedQuery) {
    return;
  }

  const normalizedQuery = trimmedQuery.toLowerCase();
  const nextItems = [
    trimmedQuery,
    ...getRecentSearchesSnapshot().filter(
      (item) => item.toLowerCase() !== normalizedQuery,
    ),
  ].slice(0, RECENT_SEARCHES_LIMIT);

  persistRecentSearches(nextItems);
}

export function subscribeRecentSearches(listener: () => void) {
  listeners.add(listener);

  return () => {
    listeners.delete(listener);
  };
}

export function useRecentSearches(): string[] {
  return useSyncExternalStore(
    subscribeRecentSearches,
    getRecentSearchesSnapshot,
    getRecentSearchesServerSnapshot,
  );
}
