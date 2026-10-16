export type PageSelectionState = {
  allSelected: boolean;
  partiallySelected: boolean;
  selectedOnPage: number;
};

function areSetsEqual(left: ReadonlySet<string>, right: ReadonlySet<string>) {
  if (left.size !== right.size) {
    return false;
  }

  for (const value of left) {
    if (!right.has(value)) {
      return false;
    }
  }

  return true;
}

export function toggleInfoHashSelection(
  selectedInfoHashes: Set<string>,
  infoHash: string,
  checked?: boolean,
) {
  const next = new Set(selectedInfoHashes);
  const shouldSelect = checked ?? !next.has(infoHash);

  if (shouldSelect) {
    next.add(infoHash);
  } else {
    next.delete(infoHash);
  }

  return areSetsEqual(selectedInfoHashes, next) ? selectedInfoHashes : next;
}

export function getPageSelectionState(
  selectedInfoHashes: ReadonlySet<string>,
  pageInfoHashes: readonly string[],
): PageSelectionState {
  const selectedOnPage = pageInfoHashes.filter((infoHash) =>
    selectedInfoHashes.has(infoHash),
  ).length;
  const allSelected = pageInfoHashes.length > 0 && selectedOnPage === pageInfoHashes.length;

  return {
    allSelected,
    partiallySelected: selectedOnPage > 0 && !allSelected,
    selectedOnPage,
  };
}

export function togglePageSelection(
  selectedInfoHashes: Set<string>,
  pageInfoHashes: readonly string[],
) {
  const next = new Set(selectedInfoHashes);
  const pageSelection = getPageSelectionState(selectedInfoHashes, pageInfoHashes);

  if (pageSelection.allSelected) {
    for (const infoHash of pageInfoHashes) {
      next.delete(infoHash);
    }
  } else {
    for (const infoHash of pageInfoHashes) {
      next.add(infoHash);
    }
  }

  return areSetsEqual(selectedInfoHashes, next) ? selectedInfoHashes : next;
}

export function clearSelectionOnSearchParamsChange(
  selectedInfoHashes: Set<string>,
  previousSearchParams: string,
  nextSearchParams: string,
) {
  return previousSearchParams === nextSearchParams ? selectedInfoHashes : new Set<string>();
}
