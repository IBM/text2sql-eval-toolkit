import React, { useEffect, useRef } from "react";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import {
  bracketMatching,
  defaultHighlightStyle,
  indentOnInput,
  syntaxHighlighting,
} from "@codemirror/language";
import { yaml as yamlLanguage } from "@codemirror/lang-yaml";
import { type Diagnostic, lintGutter, setDiagnostics } from "@codemirror/lint";
import { Compartment, EditorState } from "@codemirror/state";
import {
  EditorView,
  drawSelection,
  highlightActiveLine,
  highlightActiveLineGutter,
  keymap,
  lineNumbers,
} from "@codemirror/view";

/**
 * A small CodeMirror 6 editor.
 *
 * Assembled from the individual packages rather than the `codemirror`
 * meta-package's `basicSetup`, which pulls in autocompletion, search, folding
 * and more. None of that is wanted for a forty-line config, and all of it would
 * be weight in the chunk this lands in.
 *
 * Everything here is imported by a lazily-loaded view, so CodeMirror never
 * reaches the entry bundle — which is budgeted at 460 KB in CI and has around
 * 35 KB of headroom, far less than this needs.
 *
 * Diagnostics are supplied by the caller rather than produced by a CodeMirror
 * linter: the caller is already parsing the document to decide whether a save
 * is possible, and parsing it a second time on a different schedule would let
 * the two disagree about whether the document is valid.
 */

const language = new Compartment();
const editable = new Compartment();

/** Carbon's surfaces and type, so the editor is not a white box in a grey UI. */
const carbonTheme = EditorView.theme({
  "&": {
    fontSize: "0.8125rem",
    border: "1px solid var(--cds-border-strong)",
    backgroundColor: "var(--cds-field)",
    color: "var(--cds-text-primary)",
  },
  "&.cm-focused": {
    outline: "2px solid var(--cds-focus)",
    outlineOffset: "-2px",
  },
  ".cm-content": {
    fontFamily: '"IBM Plex Mono", ui-monospace, monospace',
    padding: "0.5rem 0",
  },
  ".cm-gutters": {
    backgroundColor: "var(--cds-layer-accent)",
    color: "var(--cds-text-secondary)",
    border: "none",
  },
  ".cm-activeLine": { backgroundColor: "var(--cds-layer-hover)" },
  ".cm-activeLineGutter": { backgroundColor: "var(--cds-layer-hover)" },
  ".cm-scroller": { overflow: "auto" },
});

interface Props {
  value: string;
  onChange: (value: string) => void;
  /** Rendered by the caller's parse; see the module comment. */
  diagnostics?: Diagnostic[];
  readOnly?: boolean;
  ariaLabel: string;
  /** Any CSS length. The editor scrolls inside it. */
  height?: string;
}

export const CodeEditor: React.FC<Props> = ({
  value,
  onChange,
  diagnostics = [],
  readOnly = false,
  ariaLabel,
  height = "28rem",
}) => {
  const host = useRef<HTMLDivElement | null>(null);
  const view = useRef<EditorView | null>(null);
  // Held in a ref so the update listener never goes stale, which would
  // otherwise mean recreating the editor -- and losing the cursor -- on every
  // keystroke. Assigned in an effect rather than during render: a ref written
  // while rendering is not guaranteed to have been written if the render is
  // discarded.
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    if (!host.current) return;

    const instance = new EditorView({
      parent: host.current,
      state: EditorState.create({
        doc: value,
        extensions: [
          lineNumbers(),
          highlightActiveLineGutter(),
          highlightActiveLine(),
          history(),
          drawSelection(),
          indentOnInput(),
          // The reason this editor exists: a config is nested maps, and a
          // missing indent or an unclosed bracket should be visible rather
          // than found by counting.
          bracketMatching(),
          syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
          lintGutter(),
          language.of(yamlLanguage()),
          // A prompt template is long prose. Without wrapping it runs off the
          // right edge and the editor becomes a one-line horizontal scroller.
          EditorView.lineWrapping,
          keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
          editable.of(EditorView.editable.of(!readOnly)),
          carbonTheme,
          EditorView.updateListener.of((update) => {
            if (update.docChanged) {
              onChangeRef.current(update.state.doc.toString());
            }
          }),
          EditorView.contentAttributes.of({
            "aria-label": ariaLabel,
            role: "textbox",
            "aria-multiline": "true",
          }),
        ],
      }),
    });
    view.current = instance;
    return () => {
      instance.destroy();
      view.current = null;
    };
    // Built once. Every prop that can change afterwards is applied by the
    // effects below, because recreating the view would drop the cursor,
    // the selection and the undo history.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Adopt a value the editor did not produce itself -- a different config
  // loaded, or the Format action rewriting the document. Comparing first is
  // what stops every keystroke from being echoed back as a full replacement.
  useEffect(() => {
    const instance = view.current;
    if (!instance) return;
    if (instance.state.doc.toString() === value) return;
    instance.dispatch({
      changes: { from: 0, to: instance.state.doc.length, insert: value },
    });
  }, [value]);

  useEffect(() => {
    view.current?.dispatch({
      effects: editable.reconfigure(EditorView.editable.of(!readOnly)),
    });
  }, [readOnly]);

  useEffect(() => {
    const instance = view.current;
    if (!instance) return;
    instance.dispatch(setDiagnostics(instance.state, diagnostics));
  }, [diagnostics]);

  return (
    <div
      ref={host}
      style={{
        height,
        overflow: "hidden",
        // The editor's own theme paints the border; this keeps the host from
        // collapsing before CodeMirror has mounted into it.
        display: "flex",
        flexDirection: "column",
      }}
    />
  );
};
