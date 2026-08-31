import { describe, expect, it } from "vitest";

import {
  formatConfig,
  missingRequiredFields,
  parseConfig,
  toYaml,
} from "./judgeConfig";

const CONFIG = `model:
  id: wxai:meta-llama/llama-4-maverick
  decoding_method: greedy
  max_new_tokens: 256

prompt_template: |
  You are an expert SQL evaluator.

  ### Question:
  {question}
`;

describe("parseConfig", () => {
  it("parses a config and keeps the block scalar's line breaks", () => {
    const result = parseConfig(CONFIG);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    const value = result.value as Record<string, any>;
    expect(value.model.id).toBe("wxai:meta-llama/llama-4-maverick");
    expect(value.prompt_template).toContain("### Question:\n{question}");
  });

  it("treats an empty document as an empty config, not a failure", () => {
    const result = parseConfig("");
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.value).toEqual({});
  });

  it("reports where the document is wrong, not just that it is", () => {
    // The whole point of the item: "invalid YAML" sends the reader looking.
    const broken = "model:\n  id: a\n   bad_indent: b\n";
    const result = parseConfig(broken);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error.line).toBeGreaterThan(1);
    expect(result.error.column).toBeGreaterThanOrEqual(1);
    expect(result.error.message).toBeTruthy();
  });

  it("gives a character range the editor can mark", () => {
    const broken = "a: [1,\n";
    const result = parseConfig(broken);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error.from).toBeGreaterThanOrEqual(0);
    expect(result.error.to).toBeGreaterThan(result.error.from!);
    expect(result.error.to).toBeLessThanOrEqual(broken.length);
  });

  it("never marks a zero-width range", () => {
    // An empty range draws nothing, so the "show me where" feature would show
    // nothing on exactly the errors reported at the end of the document.
    for (const broken of ["a: [1,\n", "one: 1\ntwo: [\n", "x: {\n"]) {
      const result = parseConfig(broken);
      if (result.ok) throw new Error(`expected a failure for ${broken}`);
      expect(result.error.to! - result.error.from!).toBeGreaterThan(0);
      expect(broken.slice(result.error.from, result.error.to)).not.toBe("\n");
    }
  });

  it("puts the offset on the reported line", () => {
    // An unterminated construct is reported by js-yaml at the phantom position
    // one past the end -- "line 4" of a three-line document.
    const broken = "one: 1\ntwo: 2\nthree: [\n";
    const result = parseConfig(broken);
    if (result.ok) throw new Error("expected a parse failure");
    const { from, line } = result.error;
    // The offset and the human-readable line must agree; computing them
    // independently is how they end up pointing at different places.
    const lineOfOffset = broken.slice(0, from).split("\n").length;
    expect(lineOfOffset).toBe(line);
    // And it must be a line the document actually has.
    expect(line).toBeLessThanOrEqual(broken.split("\n").length);
  });
});

describe("formatConfig", () => {
  it("reflows a badly laid out document", () => {
    const messy = 'model:\n    id:   "a"\nprompt_template: "x"\n';
    const result = formatConfig(messy);
    expect(result.ok).toBe(true);
    expect(result.text).toBe("model:\n  id: a\nprompt_template: x\n");
  });

  it("refuses to reformat a document it cannot parse", () => {
    const result = formatConfig("a: [1,\n");
    expect(result.ok).toBe(false);
    expect(result.text).toBeUndefined();
  });

  it("does not rewrap a long prompt", () => {
    // js-yaml folds at 80 columns by default, and a folded scalar's line
    // breaks are not the same text -- Format would silently change the prompt
    // the judge is sent.
    const long = "x".repeat(300);
    const result = formatConfig(`prompt_template: |\n  ${long}\n`);
    expect(result.ok).toBe(true);
    expect(result.text).toContain(long);
  });

  it("preserves key order rather than alphabetising", () => {
    const result = formatConfig(CONFIG);
    expect(result.text!.indexOf("model:")).toBeLessThan(
      result.text!.indexOf("prompt_template:"),
    );
  });

  it("round-trips a real config unchanged after one format", () => {
    const once = formatConfig(CONFIG).text!;
    expect(formatConfig(once).text).toBe(once);
  });
});

describe("toYaml", () => {
  it("writes a multi-line string as a block scalar, not an escaped one", () => {
    // This is the argument for editing YAML at all. As JSON the prompt is one
    // line of fifteen hundred characters with \n through it.
    const yaml = toYaml({ prompt_template: "line one\nline two\n" });
    expect(yaml).toContain("prompt_template: |");
    expect(yaml).not.toContain("\\n");
  });

  it("survives a round trip through the endpoint's JSON shape", () => {
    // The endpoint returns the parsed structure and takes JSON back; the
    // editor is the only thing that speaks YAML.
    const parsed = parseConfig(CONFIG);
    if (!parsed.ok) throw new Error("fixture does not parse");
    const viaJson = JSON.parse(JSON.stringify(parsed.value));
    const back = parseConfig(toYaml(viaJson));
    expect(back.ok).toBe(true);
    if (back.ok) expect(back.value).toEqual(parsed.value);
  });
});

describe("missingRequiredFields", () => {
  it("accepts a complete config", () => {
    const parsed = parseConfig(CONFIG);
    if (!parsed.ok) throw new Error("fixture does not parse");
    expect(missingRequiredFields(parsed.value)).toEqual([]);
  });

  it("names both when the document is empty", () => {
    expect(missingRequiredFields({})).toEqual(["model.id", "prompt_template"]);
  });

  it("names model.id when the model block has no id", () => {
    expect(missingRequiredFields({ model: {}, prompt_template: "x" })).toEqual([
      "model.id",
    ]);
  });

  it("names prompt_template when it is absent", () => {
    expect(
      missingRequiredFields({ model: { id: "anthropic:x" } }),
    ).toEqual(["prompt_template"]);
  });

  it("treats a blank value as missing rather than present", () => {
    // Valid YAML, useless judge -- which is exactly the case highlighting
    // cannot catch.
    expect(
      missingRequiredFields({ model: { id: "   " }, prompt_template: "\n" }),
    ).toEqual(["model.id", "prompt_template"]);
  });

  it("rejects a document that is not a mapping at all", () => {
    expect(missingRequiredFields(["a", "b"])).toEqual([
      "model.id",
      "prompt_template",
    ]);
    expect(missingRequiredFields(null)).toEqual([
      "model.id",
      "prompt_template",
    ]);
  });
});
