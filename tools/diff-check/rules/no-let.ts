import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

const rule: Rule = {
  meta: {
    type: "problem" as const,
    docs: {
      description: "Disallow let declarations in newly added code.",
    },
  },
  create(context) {
    return {
      VariableDeclaration(node) {
        if (node.kind !== "let") {
          return;
        }

        context.report({
          node,
          message: "Do not use let; express the value without reassignment.",
        });
      },
    };
  },
};

export default rule;
