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
 * The server refused because the caller is not signed in, not because anything
 * is missing or broken.
 *
 * A benchmark can be restricted to signed-in users, and every route that names
 * one answers 401 with `sign_in_required` to an anonymous caller. Kept distinct
 * from a plain `Error` so the UI can offer to sign in rather than report a
 * failure -- a shared link that read "not found" would look dead.
 */
export class SignInRequiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SignInRequiredError";
  }
}

/** Whether a response is the server asking the caller to sign in. */
export async function isSignInRequired(res: Response): Promise<boolean> {
  if (res.status !== 401) return false;
  try {
    const body = await res.clone().json();
    return body?.sign_in_required === true;
  } catch {
    return false;
  }
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
    } catch {
      // non-JSON body — keep the plain status message
    }
    if (await isSignInRequired(res)) {
      throw new SignInRequiredError(message);
    }
    throw new Error(message);
  }
  return res;
}
