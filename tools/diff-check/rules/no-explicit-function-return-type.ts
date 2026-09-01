import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];
type VisitorObject = ReturnType<NonNullable<Rule["create"]>>;
type FunctionDeclarationNode = Parameters<
  NonNullable<VisitorObject["FunctionDeclaration"]>
>[0];
type FunctionExpressionNode = Parameters<
  NonNullable<VisitorObject["FunctionExpression"]>
>[0];
type ArrowFunctionExpressionNode = Parameters<
  NonNullable<VisitorObject["ArrowFunctionExpression"]>
>[0];
type FunctionNode =
  | FunctionDeclarationNode
  | FunctionExpressionNode
  | ArrowFunctionExpressionNode;

const rule: Rule = {
  meta: {
    type: "problem" as const,
    docs: {
      description:
        "Disallow explicit return type annotations on function implementations.",
    },
  },
  create(context) {
    const check = (node: FunctionNode) => {
      if (node.returnType === null || node.returnType === undefined) {
        return;
      }

      context.report({
        node: node.returnType,
        message: "Do not explicitly declare a function return type; infer it.",
      });
    };

    return {
      FunctionDeclaration: check,
      FunctionExpression: check,
      ArrowFunctionExpression: check,
    };
  },
};

export default rule;
