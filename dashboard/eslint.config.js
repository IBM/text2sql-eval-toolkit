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

      // ---- Deferred, not dismissed -------------------------------------
      // 20 findings from the first lint run, all requiring effects to be
      // restructured. The routing work replaces much of this state with route
      // params and loaders, so fixing them now would be rewritten immediately.
      // Tracked as item 4.13 in docs/plan/04-code-quality.md -- re-enable and
      // clear these once routing lands.
      "react-hooks/set-state-in-effect": "off", // 19x
      "react-hooks/preserve-manual-memoization": "off", // 1x
    },
  }
);
