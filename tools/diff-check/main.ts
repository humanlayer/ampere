#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

import type { ChangedFileRule } from "./changed-file-rules.ts";
import {
  collectChangedFileViolations,
  collectViolations,
  type OxlintDiagnostic,
  parseAddedLineRanges,
  renderReport,
} from "./diff-check.ts";

export interface DiffCheckConfig {
  readonly baseRef: string;
  readonly changedFileRules: ReadonlyArray<ChangedFileRule>;
  readonly excludePathPrefixes: ReadonlyArray<string>;
  readonly oxlintConfigPath: string;
  readonly sourceExtensions: ReadonlyArray<string>;
}

const defaultConfig: DiffCheckConfig = {
  baseRef: "HEAD",
  changedFileRules: [],
  excludePathPrefixes: ["dist/", "coverage/", "vendor/"],
  oxlintConfigPath: "./diff-check.oxlintrc.json",
  sourceExtensions: [".ts", ".tsx"],
};

const stringArray = (value: unknown, name: string): ReadonlyArray<string> => {
  if (
    !Array.isArray(value) ||
    !value.every((entry) => typeof entry === "string")
  ) {
    throw new Error(`${name} must be an array of strings.`);
  }
  return value;
};

export const parseConfig = (value: unknown): DiffCheckConfig => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Diff-check config must be a JSON object.");
  }
  const candidate = value as Record<string, unknown>;
  const baseRef = candidate.baseRef ?? defaultConfig.baseRef;
  const oxlintConfigPath =
    candidate.oxlintConfigPath ?? defaultConfig.oxlintConfigPath;

  if (typeof baseRef !== "string" || baseRef.length === 0) {
    throw new Error("baseRef must be a non-empty string.");
  }
  if (typeof oxlintConfigPath !== "string" || oxlintConfigPath.length === 0) {
    throw new Error("oxlintConfigPath must be a non-empty string.");
  }
  const changedFileRules = candidate.changedFileRules ?? [];
  if (!Array.isArray(changedFileRules)) {
    throw new Error("changedFileRules must be an array.");
  }

  return {
    baseRef,
    oxlintConfigPath,
    sourceExtensions:
      candidate.sourceExtensions === undefined
        ? defaultConfig.sourceExtensions
        : stringArray(candidate.sourceExtensions, "sourceExtensions"),
    excludePathPrefixes:
      candidate.excludePathPrefixes === undefined
        ? defaultConfig.excludePathPrefixes
        : stringArray(candidate.excludePathPrefixes, "excludePathPrefixes"),
    changedFileRules: changedFileRules as ReadonlyArray<ChangedFileRule>,
  };
};

const run = (command: string, args: ReadonlyArray<string>, cwd?: string) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

const nullDelimited = (value: string) =>
  value.split("\0").filter((entry) => entry.length > 0);

export const parseArguments = (args: ReadonlyArray<string>) => {
  let baseRef: string | undefined;
  let configPath: string | undefined;
  let help = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") {
      help = true;
      continue;
    }
    if (argument === "--base" || argument === "--config") {
      const value = args[index + 1];
      if (value === undefined || value.startsWith("-")) {
        throw new Error(`${argument} requires a value.`);
      }
      if (argument === "--base") {
        baseRef = value;
      } else {
        configPath = value;
      }
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument: ${argument}`);
  }

  return { baseRef, configPath, help };
};

export const execute = () => {
  const arguments_ = parseArguments(process.argv.slice(2));
  if (arguments_.help) {
    process.stdout.write(
      [
        "Usage: ai-diff-check [--config <path>] [--base <git-ref>]",
        "",
        "Exit codes: 0 clean, 1 violations, 2 configuration or execution error.",
        "",
      ].join("\n")
    );
    return;
  }
  const root = run("git", ["rev-parse", "--show-toplevel"]).trim();
  const configPath = path.resolve(
    root,
    arguments_.configPath ?? "diff-check.config.json"
  );
  const config = parseConfig(JSON.parse(readFileSync(configPath, "utf8")));
  const baseRef = arguments_.baseRef ?? config.baseRef;
  const oxlintConfigPath = path.resolve(
    path.dirname(configPath),
    config.oxlintConfigPath
  );
  const trackedChangedFiles = nullDelimited(
    run(
      "git",
      ["diff", "--name-only", "--diff-filter=ACMRD", "-z", baseRef],
      root
    )
  );
  const trackedLintFiles = nullDelimited(
    run(
      "git",
      ["diff", "--name-only", "--diff-filter=ACMR", "-z", baseRef],
      root
    )
  );
  const untracked = nullDelimited(
    run("git", ["ls-files", "--others", "--exclude-standard", "-z"], root)
  );
  const changedFiles = [
    ...new Set([...trackedChangedFiles, ...untracked]),
  ].toSorted();
  const sourceFiles = [...new Set([...trackedLintFiles, ...untracked])].filter(
    (file) =>
      config.sourceExtensions.some((extension) => file.endsWith(extension)) &&
      !config.excludePathPrefixes.some((prefix) => file.startsWith(prefix))
  );
  const untrackedSet = new Set(untracked);
  const addedLines = new Map(
    sourceFiles.map((file) => [
      file,
      untrackedSet.has(file)
        ? [{ start: 1, end: Number.MAX_SAFE_INTEGER }]
        : parseAddedLineRanges(
            run("git", ["diff", "--unified=0", baseRef, "--", file], root)
          ),
    ])
  );
  const lintViolations =
    sourceFiles.length === 0
      ? []
      : collectViolations({
          addedLines,
          diagnostics: (
            JSON.parse(
              run(
                "oxlint",
                [
                  "-c",
                  oxlintConfigPath,
                  "-f",
                  "json",
                  ...sourceFiles.map((file) => path.join(root, file)),
                ],
                path.dirname(oxlintConfigPath)
              )
            ).diagnostics as ReadonlyArray<OxlintDiagnostic>
          ).map((diagnostic) => ({
            ...diagnostic,
            filename: path
              .relative(
                root,
                path.isAbsolute(diagnostic.filename)
                  ? diagnostic.filename
                  : path.resolve(
                      path.dirname(oxlintConfigPath),
                      diagnostic.filename
                    )
              )
              .replaceAll("\\", "/"),
          })),
        });
  const violations = [
    ...collectChangedFileViolations({
      files: changedFiles,
      rules: config.changedFileRules,
    }),
    ...lintViolations,
  ];
  process.stdout.write(
    `${renderReport({
      useColor:
        process.env.NO_COLOR === undefined && process.stdout.isTTY === true,
      violations,
    })}\n`
  );
  process.exitCode = violations.length === 0 ? 0 : 1;
};

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  try {
    execute();
  } catch (error) {
    process.stderr.write(
      `Diff check failed: ${error instanceof Error ? error.message : String(error)}\n`
    );
    process.exitCode = 2;
  }
}
