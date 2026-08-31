/**
 * Parsing, formatting and checking a judge configuration.
 *
 * **The editor edits YAML, which is what the file is.** It used to present
 * `JSON.stringify(config, null, 2)`, which had two costs. The smaller one is
 * that the thing being edited did not look like the thing being stored. The
 * larger one is `prompt_template`: it is the bulk of every config and it is
 * long multi-line prose, which YAML writes as a block scalar and JSON writes as
 * a single string of fifteen hundred characters with `\n` escapes through it.
 * Syntax highlighting does not make that editable; a different notation does.
 *
 * The endpoint is unchanged and still takes JSON — the conversion happens here.
 * Nothing is lost by it that was not lost already: the server parses the YAML
 * and re-dumps it on every save, so comments have never survived a round trip.
 */
import { dump, load, YAMLException } from "js-yaml";

export interface ParseFailure {
  message: string;
  /** 1-based, for display. Absent when the parser gave no position. */
  line?: number;
  column?: number;
  /** 0-based character offsets into the source, for the editor's marker. */
  from?: number;
  to?: number;
}

export type ParseResult =
  | { ok: true; value: unknown }
  | { ok: false; error: ParseFailure };

/** Offset of the start of `line` (0-based) within `source`. */
function lineStart(source: string, line: number): number {
  let offset = 0;
  for (let i = 0; i < line; i += 1) {
    const next = source.indexOf("\n", offset);
    if (next === -1) return source.length;
    offset = next + 1;
  }
  return offset;
}

/** 1-based line and column of a character offset. */
function positionOf(source: string, offset: number): { line: number; column: number } {
  const before = source.slice(0, offset);
  const lastNewline = before.lastIndexOf("\n");
  return {
    line: before.split("\n").length,
    column: offset - (lastNewline + 1) + 1,
  };
}

/**
 * Parse the editor's contents.
 *
 * On failure the position is carried through rather than discarded. "Invalid
 * YAML" tells the reader to go and find it; a line and column, and a marker in
 * the gutter, tells them where it is.
 */
export function parseConfig(source: string): ParseResult {
  // js-yaml raises "expected a document, but the input is empty" on a blank
  // string. The editor is blank whenever no config is selected and just after
  // a delete, and marking that as a syntax error would put a red diagnostic on
  // an empty box the user has not touched. An empty document is an empty
  // config; the required-field check is what rejects it on save.
  if (!source.trim()) return { ok: true, value: {} };

  try {
    return { ok: true, value: load(source) ?? {} };
  } catch (e: unknown) {
    if (e instanceof YAMLException && e.mark) {
      // An unterminated construct is reported at the phantom position one past
      // the last character -- "line 4" of a three-line document, with nothing
      // there to underline and a zero-width marker that would be invisible.
      // Clamping to the last real character puts both the message and the
      // marker on the line that actually has the problem.
      let from = Math.min(
        lineStart(source, e.mark.line) + e.mark.column,
        Math.max(0, source.length - 1),
      );
      // Landing on a newline is the same problem one character along: the
      // marker would sit on the line break rather than on the text.
      while (from > 0 && source[from] === "\n") from -= 1;
      // Derived from the clamped offset rather than from the mark, so the
      // line the message names and the character the editor highlights cannot
      // disagree -- computing them separately is how they end up two lines
      // apart on exactly the errors that are hardest to find by eye.
      const { line, column } = positionOf(source, from);
      const newline = source.indexOf("\n", from);
      const lineEnd = newline === -1 ? source.length : newline;
      return {
        ok: false,
        error: {
          message: e.reason || e.message,
          line,
          column,
          from,
          // Underline to the end of the offending line, never past it, and
          // never zero-width -- an empty range draws nothing.
          to: Math.min(source.length, Math.max(from + 1, lineEnd)),
        },
      };
    }
    return {
      ok: false,
      error: { message: e instanceof Error ? e.message : String(e) },
    };
  }
}

/**
 * Reformat the document: canonical indentation, block scalars kept as blocks.
 *
 * `lineWidth: -1` disables js-yaml's folding. Without it a long prompt line is
 * wrapped at 80 columns, and a folded scalar's line breaks are semantically
 * different from the original's — the Format action would silently change the
 * prompt the judge is sent.
 */
export function formatConfig(source: string): ParseResult & { text?: string } {
  const parsed = parseConfig(source);
  if (!parsed.ok) return parsed;
  return {
    ...parsed,
    text: dump(parsed.value, {
      // The file's own order is meaningful -- `model` before the prompt --
      // and alphabetising it on every save would be gratuitous churn.
      sortKeys: false,
      lineWidth: -1,
      noRefs: true,
      indent: 2,
    }),
  };
}

/** Serialise a parsed config back to YAML, for a template or a fresh load. */
export function toYaml(value: unknown): string {
  return dump(value, { sortKeys: false, lineWidth: -1, noRefs: true, indent: 2 });
}

/**
 * What the server will refuse, said here instead.
 *
 * Highlighting makes malformed YAML visible; it does nothing about a document
 * that parses cleanly and describes a useless judge. The server rejects both of
 * these with a 400, and finding that out after a round trip is a worse way to
 * learn it.
 */
export function missingRequiredFields(value: unknown): string[] {
  const missing: string[] = [];
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return ["model.id", "prompt_template"];
  }
  const config = value as Record<string, unknown>;
  const model = config.model;
  const modelId =
    typeof model === "object" && model !== null
      ? (model as Record<string, unknown>).id
      : undefined;
  if (typeof modelId !== "string" || !modelId.trim()) missing.push("model.id");
  if (
    typeof config.prompt_template !== "string" ||
    !config.prompt_template.trim()
  ) {
    missing.push("prompt_template");
  }
  return missing;
}
