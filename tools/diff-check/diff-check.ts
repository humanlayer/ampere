import type { ChangedFileRule } from "./changed-file-rules.ts";

export interface AddedLineRange {
  readonly start: number;
  readonly end: number;
}

export interface OxlintDiagnostic {
  readonly message: string;
  readonly code: string;
  readonly severity: ViolationSeverity;
  readonly filename: string;
  readonly labels: ReadonlyArray<{
    readonly span: {
      readonly line: number;
      readonly column: number;
    };
  }>;
}

export interface Violation {
  readonly rule: string;
  readonly message: string;
  readonly severity: ViolationSeverity;
  readonly filename: string;
  readonly line: number | null;
  readonly column: number | null;
}

export type ViolationSeverity = "error" | "warning" | "advice";

export const parseAddedLineRanges = (
  diff: string
): ReadonlyArray<AddedLineRange> =>
  diff.split("\n").flatMap((line) => {
    const match =
      /^@@ -\d+(?:,\d+)? \+(?<start>\d+)(?:,(?<count>\d+))? @@/.exec(line);
    const start = Number(match?.groups?.start);
    const count = Number(match?.groups?.count ?? "1");

    return Number.isFinite(start) && Number.isFinite(count) && count > 0
      ? [{ start, end: start + count - 1 }]
      : [];
  });

export const collectViolations = ({
  addedLines,
  diagnostics,
}: {
  readonly addedLines: ReadonlyMap<string, ReadonlyArray<AddedLineRange>>;
  readonly diagnostics: ReadonlyArray<OxlintDiagnostic>;
}): ReadonlyArray<Violation> =>
  diagnostics.flatMap((diagnostic) => {
    const span = diagnostic.labels.at(0)?.span;
    const ranges = addedLines.get(diagnostic.filename);

    if (
      span === undefined ||
      ranges === undefined ||
      !ranges.some(
        (range) => span.line >= range.start && span.line <= range.end
      )
    ) {
      return [];
    }

    return [
      {
        rule: diagnostic.code.replace("(", "/").replace(")", ""),
        message: diagnostic.message,
        severity: diagnostic.severity,
        filename: diagnostic.filename,
        line: span.line,
        column: span.column,
      },
    ];
  });

export const collectChangedFileViolations = ({
  files,
  rules,
}: {
  readonly files: ReadonlyArray<string>;
  readonly rules: ReadonlyArray<ChangedFileRule>;
}): ReadonlyArray<Violation> =>
  rules.flatMap((rule) =>
    files.flatMap((file) => {
      const matcher = rule.matchers.find((candidate) => {
        switch (candidate._tag) {
          case "Directory":
            return file.startsWith(candidate.path);
          case "ExtensionsExcept":
            return !candidate.extensions.some((extension) =>
              file.endsWith(extension)
            );
          case "File":
            return file === candidate.path;
        }
      });

      return matcher === undefined
        ? []
        : [
            {
              rule: rule.rule,
              message: matcher.message,
              severity: rule.severity,
              filename: file,
              line: null,
              column: null,
            },
          ];
    })
  );

const colorCodes = {
  advice: "\u001B[36m",
  bold: "\u001B[1m",
  dim: "\u001B[2m",
  error: "\u001B[31m",
  reset: "\u001B[0m",
  success: "\u001B[32m",
  warning: "\u001B[33m",
} as const;

const severityOrder: ReadonlyArray<ViolationSeverity> = [
  "error",
  "warning",
  "advice",
];

export const renderReport = ({
  useColor,
  violations,
}: {
  readonly useColor: boolean;
  readonly violations: ReadonlyArray<Violation>;
}): string => {
  const paint = (color: string, value: string) =>
    useColor ? `${color}${value}${colorCodes.reset}` : value;

  if (violations.length === 0) {
    return `Diff check\n\n  ${paint(colorCodes.success, "✓")} No violations found.`;
  }

  const details = {
    advice: { heading: "ADVICE", icon: "i", color: colorCodes.advice },
    error: { heading: "ERRORS", icon: "×", color: colorCodes.error },
    warning: { heading: "WARNINGS", icon: "⚠", color: colorCodes.warning },
  } as const;

  const sections = severityOrder.flatMap((severity) => {
    const matching = violations.filter(
      (violation) => violation.severity === severity
    );

    if (matching.length === 0) {
      return [];
    }

    const byRule = new Map<string, Array<Violation>>();
    for (const violation of matching) {
      const group = byRule.get(violation.rule) ?? [];
      group.push(violation);
      byRule.set(violation.rule, group);
    }
    const rules = [...byRule.entries()]
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([rule, ruleViolations]) => {
        const diagnostics = [...ruleViolations]
          .toSorted((left, right) =>
            `${left.filename}:${left.line ?? 0}:${left.column ?? 0}`.localeCompare(
              `${right.filename}:${right.line ?? 0}:${right.column ?? 0}`
            )
          )
          .map((violation) => {
            const location =
              violation.line === null || violation.column === null
                ? violation.filename
                : `${violation.filename}:${violation.line}:${violation.column}`;
            return `    ${paint(colorCodes.dim, location)}\n      ${violation.message}`;
          });

        return `  ${paint(details[severity].color, details[severity].icon)} ${paint(colorCodes.bold, rule)} ${paint(colorCodes.dim, `(${ruleViolations.length})`)}\n\n${diagnostics.join("\n\n")}`;
      });

    return [
      `${paint(details[severity].color, details[severity].heading)} ${paint(colorCodes.dim, `(${matching.length})`)}\n\n${rules.join("\n\n")}`,
    ];
  });

  const counts = severityOrder.flatMap((severity) => {
    const count = violations.filter(
      (violation) => violation.severity === severity
    ).length;
    return count === 0
      ? []
      : [
          `${count} ${
            severity === "advice"
              ? "advice"
              : `${severity}${count === 1 ? "" : "s"}`
          }`,
        ];
  });
  const ruleCount = new Set(violations.map(({ rule }) => rule)).size;
  const fileCount = new Set(violations.map(({ filename }) => filename)).size;

  return `Diff check\n\n${sections.join("\n\n")}\n\n${paint(colorCodes.bold, counts.join(" · "))}\nFound ${violations.length} violation${violations.length === 1 ? "" : "s"} across ${ruleCount} rule${ruleCount === 1 ? "" : "s"} in ${fileCount} file${fileCount === 1 ? "" : "s"}.`;
};
