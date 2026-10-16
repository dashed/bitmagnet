import { readdir, readFile } from "node:fs/promises";
import { gzipSync } from "node:zlib";

const kib = 1024;
const initialBudgetBytes = 250 * kib;
const lazyBudgetBytes = 150 * kib;
const assetsDir = new URL("../dist/assets/", import.meta.url);

function format(bytes) {
  return `${(bytes / kib).toFixed(1)} KiB`;
}

async function gzipSize(fileName) {
  const file = await readFile(new URL(fileName, assetsDir));

  return gzipSync(file, { level: 9 }).length;
}

const assetNames = await readdir(assetsDir);
const jsAssetNames = assetNames.filter((assetName) => assetName.endsWith(".js")).sort();
const indexAssetNames = jsAssetNames.filter((assetName) => /^index-.*\.js$/.test(assetName));
const lazyAssetNames = jsAssetNames.filter((assetName) => !indexAssetNames.includes(assetName));

if (indexAssetNames.length === 0) {
  throw new Error("No initial search-route JS chunk matched dist/assets/index-*.js");
}

const failures = [];
const initialSizes = await Promise.all(indexAssetNames.map(gzipSize));
const initialTotal = initialSizes.reduce((total, size) => total + size, 0);

console.log(
  `search route initial JS: ${format(initialTotal)} gzip / ${format(initialBudgetBytes)} budget`,
);

if (initialTotal > initialBudgetBytes) {
  failures.push(
    `search route initial JS is ${format(initialTotal)} gzip, above ${format(initialBudgetBytes)}`,
  );
}

for (const lazyAssetName of lazyAssetNames) {
  const size = await gzipSize(lazyAssetName);

  console.log(`${lazyAssetName}: ${format(size)} gzip / ${format(lazyBudgetBytes)} budget`);

  if (size > lazyBudgetBytes) {
    failures.push(`${lazyAssetName} is ${format(size)} gzip, above ${format(lazyBudgetBytes)}`);
  }
}

if (failures.length > 0) {
  for (const failure of failures) {
    console.error(failure);
  }

  process.exitCode = 1;
}
