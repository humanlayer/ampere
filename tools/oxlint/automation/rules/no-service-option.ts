import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

const message =
  "Do not use Effect.serviceOption. Require the service directly and provide it in the layer.";

const rule: Rule = {
  meta: {
    type: "problem" as const,
    docs: {
      description:
        "Avoid Effect.serviceOption; require the service directly and provide it in the layer.",
    },
  },
  create(context) {
    return {
      MemberExpression(node) {
        if (
          node.object.type === "Identifier" &&
          node.object.name === "Effect" &&
          node.property.type === "Identifier" &&
          node.property.name === "serviceOption"
        ) {
          context.report({
            node,
            message,
          });
        }
      },
    };
  },
};

export default rule;
