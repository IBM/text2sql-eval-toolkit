/**
 * The docs view's data.
 *
 * Read-only: there is no write endpoint, because the view is not an editor.
 *
 * `available: false` is not an error. `docs/` ships in neither the wheel nor
 * the sdist, so a pip install has no documents at all -- that is the intended
 * packaging, and the view has to say so rather than render a blank page.
 */
import { apiFetch, apiUrl } from "../lib/api";

export interface DocInfo {
  /** Addressable stem: `/docs/{name}`. */
  name: string;
  /** The document's own first heading. */
  title: string;
  summary: string;
}

export interface DocList {
  items: DocInfo[];
  /** False when there is no documents directory -- see the module comment. */
  available: boolean;
}

export interface Doc {
  name: string;
  title: string;
  /** Unrendered. The view renders and sanitises it. */
  markdown: string;
}

export async function fetchDocs(signal?: AbortSignal): Promise<DocList> {
  const res = await apiFetch(apiUrl("/api/docs"), { signal });
  return res.json();
}

export async function fetchDoc(
  name: string,
  signal?: AbortSignal,
): Promise<Doc> {
  const res = await apiFetch(apiUrl(`/api/docs/${encodeURIComponent(name)}`), {
    signal,
  });
  return res.json();
}
