/**
 * Resolves API paths for fetch(). With `text2sql-eval-dashboard`, the UI and
 * `/api/*` are served from the same origin. Under Vite dev, `/api` is proxied
 * to the FastAPI backend (see vite.config.ts).
 */
export function apiUrl(path: string): string {
  if (!path.startsWith("/")) {
    return `/${path}`;
  }
  return path;
}

/**
 * Fetch wrapper that throws a descriptive error on non-2xx responses.
 * Reads the FastAPI `detail` field from the response body so callers get
 * the actual server-side reason (e.g. "Summary not found") instead of just
 * the HTTP status code.
 */
export async function apiFetch(
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<Response> {
  const res = await fetch(input, init);
  if (!res.ok) {
    let message = `HTTP ${res.status}`;
    try {
      const body = await res.clone().json();
      if (body?.detail) {
        message = `HTTP ${res.status}: ${body.detail}`;
      }
    } catch (_) {
      // non-JSON body — keep the plain status message
    }
    throw new Error(message);
  }
  return res;
}
