import { eslintCompatPlugin } from "@oxlint/plugins";

import { noManualTagComparisonRule } from "#anti-slop/effect/rules/no-manual-tag-comparison";
import { noManualTaggedConstructionRule } from "#anti-slop/effect/rules/no-manual-tagged-construction";
import { noServiceConstructorImportsRule } from "#anti-slop/effect/rules/no-service-constructor-imports";
import { preferEffectMatchRule } from "#anti-slop/effect/rules/prefer-effect-match";
import { preferTaggedErrorHandlingRule } from "#anti-slop/effect/rules/prefer-tagged-error-handling";

/** Opt-in Oxlint rules for Effect service and Layer architecture. */
const antiSlopEffectPlugin = eslintCompatPlugin({
	meta: { name: "anti-slop-effect" },
	rules: {
		"no-manual-tag-comparison": noManualTagComparisonRule,
		"no-manual-tagged-construction": noManualTaggedConstructionRule,
		"no-service-constructor-imports": noServiceConstructorImportsRule,
		"prefer-effect-match": preferEffectMatchRule,
		"prefer-tagged-error-handling": preferTaggedErrorHandlingRule,
	},
});

export default antiSlopEffectPlugin;
