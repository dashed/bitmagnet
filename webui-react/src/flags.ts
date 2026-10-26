type FlagValue = boolean | number | string | null | undefined;

type BitmagnetRuntimeFlags = {
  VITE_ENABLE_SEARCH_MODES?: FlagValue;
  enableSearchModes?: FlagValue;
};

declare global {
  interface Window {
    __BITMAGNET_FLAGS__?: BitmagnetRuntimeFlags;
  }
}

function parseFlagValue(value: FlagValue): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value !== "string") {
    return undefined;
  }

  switch (value.trim().toLowerCase()) {
    case "0":
    case "false":
    case "no":
    case "off":
      return false;
    case "1":
    case "true":
    case "yes":
    case "on":
      return true;
    default:
      return undefined;
  }
}

function getBuildTimeSearchModesDefault() {
  const buildValue = import.meta.env["VITE_ENABLE_SEARCH_MODES"] as FlagValue;

  return parseFlagValue(buildValue) ?? true;
}

export function isSearchModesEnabled() {
  const runtimeFlags = window.__BITMAGNET_FLAGS__;
  const runtimeOverride =
    parseFlagValue(runtimeFlags?.enableSearchModes) ??
    parseFlagValue(runtimeFlags?.VITE_ENABLE_SEARCH_MODES);

  return runtimeOverride ?? getBuildTimeSearchModesDefault();
}
