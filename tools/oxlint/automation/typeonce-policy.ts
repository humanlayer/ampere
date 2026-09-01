import { allRules, profile } from "./profiles.ts";
import type { RuleProfile } from "./catalog.ts";

export interface ConfigOptions {
  readonly namespace?: string;
  readonly pluginSpecifier?: string;
  readonly profiles?: ReadonlyArray<RuleProfile>;
  readonly ignorePatterns?: ReadonlyArray<string>;
  readonly stylesheetEntryPoint?: string;
  readonly restrictedClasses?: ReadonlyArray<string>;
}

const builtInRules = {
  "no-console": "error",
  "no-empty": "error",
  "no-empty-function": "warn",
  "no-eq-null": "error",
  "no-unused-vars": "warn",
  "unicorn/filename-case": "error",
  "require-yield": "off",
  "no-shadow": "off",
  "no-img-element": "off",
  "react-in-jsx-scope": "off",
  "exhaustive-deps": "off",
} as const;

const documentedDisabledRules = {
  "no-underscore-dangle": "off",
} as const;

const tailwindRules = {
  "better-tailwindcss/no-concatenated-classes": "error",
  "better-tailwindcss/no-conflicting-classes": "error",
  "better-tailwindcss/no-deprecated-classes": "error",
  "better-tailwindcss/no-duplicate-classes": "error",
  "better-tailwindcss/no-restricted-classes": "error",
  "better-tailwindcss/no-unknown-classes": "error",
} as const;

const defaultRestrictedClasses = ["\\[[^\\]]+\\]"] as const;

export const createConfig = ({
  ignorePatterns = [
    "dist/**",
    "coverage/**",
    ".next/**",
    "*.js",
    "*.jsx",
    "*.mjs",
    "*.cjs",
  ],
  namespace = "automation",
  pluginSpecifier = "./rules/oxlint/src/index.ts",
  profiles,
  restrictedClasses = defaultRestrictedClasses,
  stylesheetEntryPoint = "./src/styles.css",
}: ConfigOptions = {}) => {
  const tailwindEnabled =
    profiles === undefined || profiles.includes("next-tailwind");
  const customRules =
    profiles === undefined
      ? namespace === "automation"
        ? allRules
        : Object.fromEntries(
            Object.entries(allRules).map(([name, severity]) => [
              name.replace(/^automation\//, `${namespace}/`),
              severity,
            ])
          )
      : Object.assign(
          {},
          ...profiles.map((profileName) => profile(profileName, namespace))
        );

  return {
    $schema: "./node_modules/oxlint/configuration_schema.json",
    ignorePatterns,
    jsPlugins: [
      pluginSpecifier,
      ...(tailwindEnabled ? ["eslint-plugin-better-tailwindcss"] : []),
    ],
    categories: {
      correctness: "error",
      suspicious: "warn",
    },
    settings: tailwindEnabled
      ? {
          "better-tailwindcss": {
            entryPoint: stylesheetEntryPoint,
          },
        }
      : {},
    rules: {
      ...customRules,
      ...builtInRules,
      ...(tailwindEnabled
        ? {
            ...tailwindRules,
            "better-tailwindcss/no-restricted-classes": [
              "error",
              {
                restrict: restrictedClasses,
              },
            ],
          }
        : {}),
    },
  } as const;
};

export {
  builtInRules,
  defaultRestrictedClasses,
  documentedDisabledRules,
  tailwindRules,
};
