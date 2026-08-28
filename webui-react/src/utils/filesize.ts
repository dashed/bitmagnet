const FILE_SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;

export function formatFileSize(bytes: number) {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "unknown";
  }

  if (bytes === 0) {
    return "0 B";
  }

  let value = bytes;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < FILE_SIZE_UNITS.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  const precision = unitIndex === 0 || value >= 10 ? 0 : 1;

  return `${value.toFixed(precision)} ${FILE_SIZE_UNITS[unitIndex]}`;
}
