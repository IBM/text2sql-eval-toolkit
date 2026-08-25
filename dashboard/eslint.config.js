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
      "react-hooks/set-state-in-effect": "off", // 20x, see above
      "react-hooks/preserve-manual-memoization": "off", // 1x, see above

      // ---- Still off, with a reason ------------------------------------
      // 21 findings. Re-assessed after routing landed (which was the original
      // reason for deferring) and they did not go away: 5 are the
      // fetch-on-mount pattern, where setState happens in an async callback --
      // the sanctioned use of an effect, which this rule cannot distinguish --
      // and 15 are genuinely synchronous "reset the selection when the options
      // load".
      //
      // Those 15 are real debt and belong in derived state. They are not being
      // rewritten blind: each decides which option a user ends up looking at,
      // and there are no component tests to catch a change. Component coverage
      // comes first (plan item 4.5), then these.
      //
      // Tracked as item 4.13 in docs/plan/04-code-quality.md.

    },
  }
);
