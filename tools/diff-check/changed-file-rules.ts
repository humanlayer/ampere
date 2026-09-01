import type { ViolationSeverity } from "./diff-check.ts";

export type ChangedFileMatcher =
  | {
      readonly _tag: "File";
      readonly path: string;
      readonly message: string;
    }
  | {
      readonly _tag: "Directory";
      readonly path: string;
      readonly message: string;
    }
  | {
      readonly _tag: "ExtensionsExcept";
      readonly extensions: ReadonlyArray<string>;
      readonly message: string;
    };

export interface ChangedFileRule {
  readonly rule: string;
  readonly severity: ViolationSeverity;
  readonly matchers: ReadonlyArray<ChangedFileMatcher>;
}

export const changedFileRules: ReadonlyArray<ChangedFileRule> = [
  {
    rule: "diff/key-file-change",
    severity: "warning",
    matchers: [
      {
        _tag: "Directory",
        path: "database/migrations/",
        message: "A database migration changed.",
      },
      {
        _tag: "File",
        path: "config/runtime.ts",
        message: "A runtime configuration file changed.",
      },
      {
        _tag: "ExtensionsExcept",
        extensions: [".ts", ".tsx"],
        message: "A non-TypeScript file changed.",
      },
    ],
  },
];
