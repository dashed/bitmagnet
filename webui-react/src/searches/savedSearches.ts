import { useSyncExternalStore } from "react";

import {
  parseTorrentSearchParams,
  stringifyTorrentSearchParams,
  type TorrentSearchUrlParams,
} from "../routes/searchParams";

export type SavedSearch = {
  createdAt: number;
  id: string;
  name: string;
  params: TorrentSearchUrlParams;
};

type SavedSearchesStorage = {
  items: SavedSearch[];
  version: 1;
};

const EMPTY_SAVED_SEARCHES: SavedSearch[] = [];
const SAVED_SEARCHES_STORAGE_KEY = "bitmagnet-saved-searches";
const SAVED_SEARCHES_STORAGE_VERSION = 1;
const listeners = new Set<() => void>();

let cachedSnapshot: SavedSearch[] | undefined;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const prototype: unknown = Object.getPrototypeOf(value);

  return prototype === null || prototype === Object.prototype;
}

function sanitizeSavedSearch(value: unknown): SavedSearch | null {
  if (
    !isPlainObject(value) ||
    typeof value["createdAt"] !== "number" ||
    !Number.isFinite(value["createdAt"]) ||
    typeof value["id"] !== "string" ||
    typeof value["name"] !== "string" ||
    !isPlainObject(value["params"])
  ) {
    return null;
  }

  return {
    createdAt: value["createdAt"],
    id: value["id"],
    name: value["name"],
    params: stringifyTorrentSearchParams(parseTorrentSearchParams(value["params"])),
  };
}

function parseSavedSearches(rawValue: string | null): SavedSearch[] {
  if (!rawValue) {
    return [];
  }

  try {
    const value: unknown = JSON.parse(rawValue);

    if (
      !isPlainObject(value) ||
      !Array.isArray(value["items"]) ||
      value["version"] !== SAVED_SEARCHES_STORAGE_VERSION
    ) {
      return [];
    }

    const items: SavedSearch[] = [];

    for (const item of value["items"]) {
      const savedSearch = sanitizeSavedSearch(item);

      if (savedSearch) {
        items.push(savedSearch);
      }
    }

    return items;
  } catch {
    return [];
  }
}

function readSavedSearches(): SavedSearch[] {
  if (typeof window === "undefined") {
    return EMPTY_SAVED_SEARCHES;
  }

  try {
    return parseSavedSearches(window.localStorage.getItem(SAVED_SEARCHES_STORAGE_KEY));
  } catch {
    return [];
  }
}

function createSavedSearchId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function notifySavedSearchesListeners() {
  for (const listener of listeners) {
    listener();
  }
}

function persistSavedSearches(items: SavedSearch[]) {
  const storage: SavedSearchesStorage = {
    items,
    version: SAVED_SEARCHES_STORAGE_VERSION,
  };

  if (typeof window !== "undefined") {
    window.localStorage.setItem(SAVED_SEARCHES_STORAGE_KEY, JSON.stringify(storage));
  }

  cachedSnapshot = items;
  notifySavedSearchesListeners();
}

function getSavedSearchesServerSnapshot() {
  return EMPTY_SAVED_SEARCHES;
}

function handleSavedSearchesStorage(event: StorageEvent) {
  if (event.key !== null && event.key !== SAVED_SEARCHES_STORAGE_KEY) {
    return;
  }

  cachedSnapshot = undefined;
  notifySavedSearchesListeners();
}

if (typeof window !== "undefined") {
  window.addEventListener("storage", handleSavedSearchesStorage);
}

export function getSavedSearchesSnapshot(): SavedSearch[] {
  cachedSnapshot ??= readSavedSearches();

  return cachedSnapshot;
}

export function subscribeSavedSearches(listener: () => void) {
  listeners.add(listener);

  return () => {
    listeners.delete(listener);
  };
}

export function addSavedSearch(
  name: string,
  params: TorrentSearchUrlParams,
): SavedSearch | undefined {
  const trimmedName = name.trim();

  if (!trimmedName) {
    return undefined;
  }

  const currentItems = getSavedSearchesSnapshot();
  const existingIndex = currentItems.findIndex(
    (item) => item.name.toLowerCase() === trimmedName.toLowerCase(),
  );
  const sanitizedParams = stringifyTorrentSearchParams(parseTorrentSearchParams(params));
  let item: SavedSearch;
  let nextItems: SavedSearch[];

  if (existingIndex >= 0) {
    const existingItem = currentItems[existingIndex];

    item = {
      createdAt: existingItem.createdAt,
      id: existingItem.id,
      name: trimmedName,
      params: sanitizedParams,
    };
    nextItems = currentItems.map((currentItem, index) =>
      index === existingIndex ? item : currentItem,
    );
  } else {
    item = {
      createdAt: Date.now(),
      id: createSavedSearchId(),
      name: trimmedName,
      params: sanitizedParams,
    };
    nextItems = [...currentItems, item];
  }

  persistSavedSearches(nextItems);

  return item;
}

export function renameSavedSearch(id: string, name: string): SavedSearch | undefined {
  const trimmedName = name.trim();

  if (!trimmedName) {
    return undefined;
  }

  const currentItems = getSavedSearchesSnapshot();
  const existingItem = currentItems.find((item) => item.id === id);

  if (!existingItem) {
    return undefined;
  }

  const renamedItem: SavedSearch = {
    createdAt: existingItem.createdAt,
    id: existingItem.id,
    name: trimmedName,
    params: existingItem.params,
  };
  const nextItems = currentItems.map((item) => (item.id === id ? renamedItem : item));

  persistSavedSearches(nextItems);

  return renamedItem;
}

export function deleteSavedSearch(id: string) {
  const currentItems = getSavedSearchesSnapshot();
  const nextItems = currentItems.filter((item) => item.id !== id);

  if (nextItems.length === currentItems.length) {
    return;
  }

  persistSavedSearches(nextItems);
}

export function useSavedSearches(): SavedSearch[] {
  return useSyncExternalStore(
    subscribeSavedSearches,
    getSavedSearchesSnapshot,
    getSavedSearchesServerSnapshot,
  );
}
