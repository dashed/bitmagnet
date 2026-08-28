import type { CodegenConfig } from "@graphql-codegen/cli";

const config: CodegenConfig = {
  schema: ["../graphql/schema/**/*.graphqls"],
  documents: ["src/**/*.graphql"],
  hooks: {
    afterAllFileWrite: ["node ./scripts/patch-codegen-output.mjs"],
  },
  generates: {
    "src/graphql/generated/": {
      preset: "client",
      presetConfig: {
        // Fragment masking adds more ergonomic tax than value for this small team and app scale.
        fragmentMasking: false,
      },
      config: {
        documentMode: "string",
        enumsAsTypes: true,
        skipTypename: false,
        strictScalars: true,
        scalars: {
          Date: "string",
          DateTime: "string",
          Duration: "string",
          Hash20: "string",
          Hash32: "string",
          Void: "null | undefined",
          Year: "number",
        },
      },
    },
  },
};

export default config;
