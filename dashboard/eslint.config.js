// Flat ESLint config for the dashboard.
//
// package.json has always declared a `lint` script, but ESLint was never a
// dependency, so `npm run lint` failed and nothing enforced it. This config is
// deliberately conservative: correctness rules that catch real mistakes, with
// stylistic opinions left to review. Widen it in its own change.

import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.browser,
    },
    plugins: { "react-hooks": reactHooks },
    rules: {
      ...reactHooks.configs.recommended.rules,
      // The codebase uses `any` in places where Carbon's types are loose.
      // Tightening these is tracked separately rather than blocking CI now.
      "@typescript-eslint/no-explicit-any": "off",
      // Unused args prefixed with _ are intentional.
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "react-hooks/set-state-in-effect": "off", // 16x, see above
      "react-hooks/preserve-manual-memoization": "off", // 1x, see above

      // ---- Still off, and shrinking ------------------------------------
      // 21 findings originally; 17 now. The metric- and pipeline-selection
      // effects in ToolkitInsightsView, PipelineCompareView and
      // ProfileCompareView have been converted to derived state, with the rule
      // extracted to lib/metricInsightsSelect and unit-tested, so those three
      // views no longer correct a selection after rendering an invalid one.
      //
      // What remains: 5 are the fetch-on-mount pattern, where setState happens
      // in an async callback -- the sanctioned use of an effect, which this
      // rule cannot distinguish. The rest are per-view state resets that need a
      // component test each before being rewritten, since every one of them
      // decides what a user ends up looking at.
      //

    },
  }
);
