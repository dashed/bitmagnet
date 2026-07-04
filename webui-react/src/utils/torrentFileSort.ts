export type FileSortDirection = "asc" | "desc";
export type FileSortField = "index" | "path" | "type" | "size";

export type FileSort = {
  direction: FileSortDirection;
  field: FileSortField;
};

export type SortableFileRow = {
  fileType?: string | null;
  index: number;
  path: string;
  size: number;
};

const TEXT_COMPARE_OPTIONS = { sensitivity: "base" } satisfies Intl.CollatorOptions;

function compareText(left: string, right: string) {
  return left.localeCompare(right, undefined, TEXT_COMPARE_OPTIONS);
}

export function compareFileRowsByIndex(left: SortableFileRow, right: SortableFileRow) {
  return left.index - right.index;
}

export function compareFileRowsByPath(left: SortableFileRow, right: SortableFileRow) {
  return compareText(left.path, right.path);
}

export function compareFileRowsByType(left: SortableFileRow, right: SortableFileRow) {
  return compareText(left.fileType ?? "unknown", right.fileType ?? "unknown");
}

export function compareFileRowsBySize(left: SortableFileRow, right: SortableFileRow) {
  return left.size - right.size;
}

function compareFileRowsAscending(
  left: SortableFileRow,
  right: SortableFileRow,
  field: FileSortField,
) {
  switch (field) {
    case "index":
      return compareFileRowsByIndex(left, right);
    case "path":
      return compareFileRowsByPath(left, right);
    case "type":
      return compareFileRowsByType(left, right);
    case "size":
      return compareFileRowsBySize(left, right);
  }
}

export function compareFileRows(left: SortableFileRow, right: SortableFileRow, sort: FileSort) {
  const direction = sort.direction === "asc" ? 1 : -1;

  return compareFileRowsAscending(left, right, sort.field) * direction;
}
