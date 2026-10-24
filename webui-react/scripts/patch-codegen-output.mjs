import { readFileSync, writeFileSync } from "node:fs";
import { URL } from "node:url";

const generatedGraphqlPath = new URL("../src/graphql/generated/graphql.ts", import.meta.url);
const source = readFileSync(generatedGraphqlPath, "utf8");
const patched = source.replace(
  "\n  toString(): string & DocumentTypeDecoration<TResult, TVariables> {",
  "\n  override toString(): string & DocumentTypeDecoration<TResult, TVariables> {",
);

if (patched !== source) {
  writeFileSync(generatedGraphqlPath, patched);
}
