import noExplicitFunctionReturnType from "./rules/no-explicit-function-return-type.ts";
import noLet from "./rules/no-let.ts";
import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

interface Plugin {
  readonly meta: {
    readonly name: string;
  };
  readonly rules: Readonly<Record<string, Rule>>;
}

const plugin: Plugin = {
  meta: {
    name: "diff-check",
  },
  rules: {
    "no-explicit-function-return-type": noExplicitFunctionReturnType,
    "no-let": noLet,
  },
};

export default plugin;
