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
