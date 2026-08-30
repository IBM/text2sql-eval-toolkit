import argparse
import os
import subprocess
import threading
from typing import Dict, List, Optional

from fastapi import FastAPI, Request
import uvicorn

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit import __version__
from text2sql_eval_toolkit.ui import auth
from text2sql_eval_toolkit.ui.models import (
    DeploymentInfo,
    SessionInfo,
)
from text2sql_eval_toolkit.ui.judge_budget import (
    judge_disabled,
)
from text2sql_eval_toolkit.ui.capabilities import (
    Tier,
    resolve_tier,
)
from text2sql_eval_toolkit.ui.user_keys import (
    SECRET_KEY_ENV,
    UserKeyStore,
    secrets_available,
)
from text2sql_eval_toolkit.ui.roles import (
    ADMIN_EMAILS_ENV,
    REMOVED_ALLOWLIST_ENV,
    ROLE_TIERS,
    Role,
    UserStore,
    admin_emails_from_env,
    effective_role,
)

# Runtime state (deployment ceiling, judge allowlist, request identity) and the
# middleware stack live in their own modules.  Their names are re-exported here
# because ``server.set_mode`` / ``server.reset_rate_limits`` are the surface the
# CLI and the tests already use -- and re-exporting is only safe because they are
# accessors over module state rather than the state itself.
from text2sql_eval_toolkit.ui import middleware as _middleware
from text2sql_eval_toolkit.ui import runtime as _runtime
from text2sql_eval_toolkit.ui import (
    routers_auth,
    routers_benchmarks,
    routers_errors,
    routers_jobs,
    routers_results,
    routers_compare,
    routers_execution,
    routers_judge,
    routers_judge_configs,
    routers_users,
    routers_keys,
    static_files,
)
from text2sql_eval_toolkit.ui.indexes import (  # noqa: F401
    EVAL_INDEX_CACHE,
    get_index,
    invalidate_index_cache,
)
from text2sql_eval_toolkit.ui.paths import (  # noqa: F401
    _eval_not_found_detail,
    _summary_not_found_detail,
    count_records,
    get_data_root,
    get_results_dir,
    load_json,
)
from text2sql_eval_toolkit.ui.registry import (  # noqa: F401
    ALLOWED_DB_TYPES,
    ALLOWED_LOGO_EXTENSIONS,
    MAX_LOGO_UPLOAD_BYTES,
    STATIC_ASSET_SUBDIR,
    get_benchmark_registry_path,
    load_benchmark_registry,
    normalize_benchmark_config,
    normalize_benchmark_id,
    write_json_atomic,
)
from text2sql_eval_toolkit.ui.middleware import reset_rate_limits  # noqa: F401
from text2sql_eval_toolkit.ui.runtime import (  # noqa: F401
    _cookie_secure,
    current_user_email,
    get_admin_emails,
    get_mode,
    set_admin_emails,
    set_mode,
)
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

app = FastAPI(title="Text2SQL Evaluation Dashboard API")

# When True the /api/results/fetch endpoints are active.  Set by main() via
# the --enable-fetch CLI flag.  Off by default so production deployments are
# safe without any configuration.
_ENABLE_FETCH_ENDPOINT: bool = False

_middleware.install(app)

# Routes are grouped by what they can do rather than by URL shape: the execution
# and judge routers are the two that carry capability beyond reading artifacts,
# and keeping each in one file is what makes that reviewable.
for _router in (
    routers_auth.router,
    routers_judge.router,
    routers_benchmarks.router,
    routers_errors.router,
    routers_jobs.router,
    routers_results.router,
    routers_execution.router,
    routers_compare.router,
    routers_judge_configs.router,
    routers_users.router,
    routers_keys.router,
    static_files.router,
):
    app.include_router(_router)

# Names main() and the tests reach for, kept on `server` so moving a definition
# is not also an API change for callers.
SPAStaticFiles = static_files.SPAStaticFiles
mount_static = static_files.mount_static
_resolve_dashboard_source_dir = static_files._resolve_dashboard_source_dir
_ensure_dashboard_dist = static_files._ensure_dashboard_dist
_spawn_dashboard_watch = static_files._spawn_dashboard_watch
_terminate_dashboard_watch = static_files._terminate_dashboard_watch
get_judge_store = routers_judge.get_judge_store
reset_judge_store = routers_judge.reset_judge_store
_judge_config_dir = routers_judge._judge_config_dir
get_oauth = routers_auth.get_oauth
_judge_usage_model = routers_judge._judge_usage_model


def configure_cors(mode: Tier) -> None:
    """Narrow CORS for shared deployments. See ``ui.middleware``."""
    _middleware.configure_cors(app, mode)


@app.get("/api/me", response_model=SessionInfo)
def get_session_info(request: Request) -> SessionInfo:
    """
    The caller's effective capability.

    The UI uses this to hide actions that would 403, so a read-only visitor is
    not offered buttons that cannot work.
    """
    email = current_user_email(request)
    role = effective_role(email, _runtime.get_user_store(), get_admin_emails())
    tier = resolve_tier(
        get_mode(), email, ROLE_TIERS[role], _runtime.is_remote_deployment()
    )
    judge_usage = None
    if tier >= Tier.JUDGE and not judge_disabled():
        try:
            judge_usage = _judge_usage_model(get_judge_store().usage())
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Could not read judge usage: %s", exc)

    return SessionInfo(
        role=role.value,
        # Mirrors the middleware gate: a full-mode operator already controls the
        # process, so the console is theirs without configuration.
        can_manage_users=(
            role is Role.ADMIN
            or (get_mode() is Tier.FULL and not _runtime.is_remote_deployment())
        ),
        tier=tier.name.lower(),
        mode=get_mode().name.lower(),
        email=email,
        signed_in=bool(email),
        # Reported false when the kill switch is on, so the UI stops offering an
        # action that would 503.
        can_run_judge=tier >= Tier.JUDGE and not judge_disabled(),
        can_mutate=tier >= Tier.FULL,
        judge_usage=judge_usage,
    )


def _read_provisioning_marker() -> Dict[str, str]:
    """Parse the marker deploy/provision.sh leaves behind, if present."""
    marker = get_data_root() / ".provisioned"
    values: Dict[str, str] = {}
    try:
        for line in marker.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


@app.get("/api/deployment", response_model=DeploymentInfo)
def get_deployment_info() -> DeploymentInfo:
    """
    Describe the deployment itself, as opposed to the caller.

    Separate from /api/me because it is the same for everyone and changes only
    on redeploy, and because the UI needs it before anyone signs in.
    """
    marker = _read_provisioning_marker()
    return DeploymentInfo(
        mode=get_mode().name.lower(),
        toolkit_version=__version__,
        data_revision=marker.get("revision"),
        data_provisioned_at=marker.get("provisioned_at"),
        sign_in_available=auth.is_configured(),
        judge_available=get_mode() >= Tier.JUDGE and not judge_disabled(),
    )


def main(argv: Optional[List[str]] = None) -> None:
    """
    Console entrypoint that starts the API (and static UI if built),
    intended to be wired as `text2sql-eval-dashboard`.
    """
    dashboard_dir = _resolve_dashboard_source_dir()
    default_watch = dashboard_dir is not None

    parser = argparse.ArgumentParser(
        description="Run the Text2SQL Evaluation Dashboard"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--forwarded-allow-ips",
        default=os.getenv("TEXT2SQL_FORWARDED_ALLOW_IPS", "127.0.0.1"),
        help=(
            "Peers whose X-Forwarded-* headers are believed, as IPs or CIDRs "
            "(comma-separated), or '*' for any. Behind a TLS-terminating proxy "
            "this MUST include the proxy, or the app sees every request as "
            "http and builds an http OAuth redirect_uri that Google rejects. "
            "Defaults to 127.0.0.1, which is right for a local run and wrong "
            "for a proxy in another container."
        ),
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the default browser to the dashboard URL after startup",
    )
    parser.add_argument(
        "--watch-dashboard",
        action=argparse.BooleanOptionalAction,
        default=default_watch,
        help=(
            "Watch dashboard sources and rebuild dashboard/dist via `vite build --watch` (requires npm). "
            "Defaults to on when a dashboard/ tree with package.json is found next to the repo or cwd; "
            "use --no-watch-dashboard to serve existing dist only."
        ),
    )
    parser.add_argument(
        "--mode",
        default=os.getenv("TEXT2SQL_DASHBOARD_MODE", "full"),
        choices=[t.name.lower() for t in Tier],
        help=(
            "Capability ceiling for this deployment. 'full' (default) is the "
            "local operator tool and is refused on a non-loopback interface "
            "without --allow-remote-full. 'public' serves pre-computed results "
            "read-only. 'judge' additionally lets allowlisted signed-in users "
            "run LLM-as-judge."
        ),
    )
    parser.add_argument(
        "--allow-remote-full",
        action="store_true",
        help=(
            "Permit --mode full on a non-loopback interface. This exposes SQL "
            "execution and registry writes to anyone who can reach the port; "
            "do not use it to serve the public dashboard."
        ),
    )
    parser.add_argument(
        "--enable-fetch",
        action="store_true",
        default=False,
        help=(
            "Enable the /api/results/fetch endpoint and the in-dashboard "
            "'Fetch results' button.  Off by default; intended for developer "
            "or controlled environments only.  Production deployments should "
            "use `text2sql-eval-toolkit results fetch` from the CLI instead."
        ),
    )
    args = parser.parse_args(argv)

    mode = Tier.parse(args.mode)
    # The dangerous configuration should take deliberate effort, not a default.
    loopback = args.host in {"127.0.0.1", "localhost", "::1"}
    # Recorded so authorization can tell the two meanings of `full` apart:
    # on a laptop it is the operator, who already controls this process; on a
    # reachable host it must mean "signed in and granted the role", or it would
    # hand SQL execution to anyone who finds the URL.
    _runtime.set_remote_deployment(not loopback)
    if mode is Tier.FULL and not loopback and not args.allow_remote_full:
        parser.error(
            f"--mode full refuses to bind {args.host}: it exposes SQL execution "
            "against configured database credentials, evaluation runs, and "
            "registry writes to anyone who can reach the port. Use --mode public "
            "for a shared deployment, or --allow-remote-full if you are certain."
        )
    if mode is Tier.FULL and not loopback:
        logger.warning(
            "Running --mode full on %s. The ceiling is raised, but full is NOT "
            "granted to anonymous callers here: a caller must be signed in and "
            "hold the 'full' or 'admin' role. Grant it from the Users console.",
            args.host,
        )
    set_mode(mode)
    set_admin_emails(admin_emails_from_env())
    _runtime.set_user_store(UserStore(get_data_root() / "users" / "roles.sqlite"))
    if secrets_available():
        _runtime.set_user_key_store(
            UserKeyStore(get_data_root() / "users" / "keys.sqlite")
        )
    elif mode is not Tier.FULL:
        logger.info(
            "%s is not set, so users cannot store their own provider keys; "
            "requests use the server credential.",
            SECRET_KEY_ENV,
        )
    configure_cors(mode)

    if auth.is_configured():
        from starlette.middleware.sessions import SessionMiddleware

        # Lax rather than Strict: the OAuth callback is a cross-site redirect
        # back to us, and Strict would withhold the cookie and break the state
        # check. https_only is off for a loopback dev run and must be on behind
        # TLS, which is what the deployment terminates at.
        app.add_middleware(
            SessionMiddleware,
            secret_key=auth.session_secret(),
            session_cookie="t2s_session",
            max_age=auth.SESSION_MAX_AGE_SECONDS,
            same_site="lax",
            # Driven by the deployment mode, not the bind address: behind a
            # TLS-terminating proxy the app binds an internal address, which
            # would have silently dropped Secure on exactly the deployment that
            # needs it. Override only for a local HTTP experiment.
            https_only=_cookie_secure(mode),
        )
        logger.info("Google sign-in enabled")
    elif mode is not Tier.FULL:
        logger.warning(
            "Mode is '%s' but Google sign-in is not configured, so nobody can "
            "reach the judge tier. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.",
            mode.name.lower(),
        )

    admins = get_admin_emails()

    # TEXT2SQL_ADMIN_EMAILS is the only env-level authority now, and therefore
    # the only recovery path. A shared deployment with none has nobody who can
    # grant a role, and no way to fix that short of a redeploy -- refuse rather
    # than start something that cannot be administered.
    if not admins and mode is not Tier.FULL:
        parser.error(
            f"--mode {mode.name.lower()} needs at least one administrator, and "
            f"{ADMIN_EMAILS_ENV} is empty. Nobody could grant a role, and there "
            "is no other way in. Set it to one or more verified email addresses."
        )

    # Removed in 1.4.0. An operator who upgrades without reading the notes would
    # otherwise watch it silently stop working.
    if os.getenv(REMOVED_ALLOWLIST_ENV):
        logger.warning(
            "%s is set but no longer used: roles moved to the database in 1.4.0. "
            "Grant the judge role from the dashboard, or list the address in %s. "
            "Unset it to silence this.",
            REMOVED_ALLOWLIST_ENV,
            ADMIN_EMAILS_ENV,
        )

    # The mode is a ceiling, so a `public` deployment grants `public` to
    # everyone -- whatever any role says. That is the correct fail-closed
    # default, but it is silent: the judge control simply never appears. Say it.
    if admins and mode < Tier.JUDGE:
        logger.warning(
            "Mode is '%s', which is a ceiling, so no role grants anything above "
            "'%s' -- including the %d administrator(s). Set "
            "TEXT2SQL_DASHBOARD_MODE=judge to let roles take effect.",
            mode.name.lower(),
            mode.name.lower(),
            len(admins),
        )
    logger.info(
        "Capability mode: %s (%d administrator%s)",
        mode.name.lower(),
        len(admins),
        "" if len(admins) == 1 else "s",
    )

    if args.enable_fetch:
        _runtime.set_fetch_endpoint_enabled(True)
        logger.info(
            "Results fetch endpoint enabled.  " "POST /api/results/fetch is active."
        )

    # Check whether results are present; hint if not.
    data_root = get_data_root()
    results_dir = data_root / "results"
    if not results_dir.is_dir() or not any(results_dir.iterdir()):
        logger.info(
            "No results found at %s.  Run: text2sql-eval-toolkit results fetch",
            results_dir,
        )

    watch_proc: Optional[subprocess.Popen] = None
    try:
        if args.watch_dashboard:
            if dashboard_dir is None:
                logger.warning(
                    "--watch-dashboard is enabled but no dashboard/package.json was found; "
                    "skipping watch. Use --no-watch-dashboard to silence this."
                )
            else:
                _ensure_dashboard_dist(dashboard_dir)
                watch_proc = _spawn_dashboard_watch(dashboard_dir)

        mount_static(app)

        if args.open_browser:
            import webbrowser

            url = f"http://{args.host}:{args.port}"
            # Open slightly after startup; this is best-effort.
            threading.Timer(1.5, lambda: webbrowser.open(url)).start()

        # proxy_headers is on by default, but its trust list is not: uvicorn
        # believes X-Forwarded-* only from 127.0.0.1 unless told otherwise, and
        # a proxy in another container is not that. Left unset, the app reports
        # scheme http behind TLS -- so `url_for("auth_callback")` produces an
        # http redirect_uri that Google refuses, and every visitor shares one
        # rate-limit bucket keyed on the proxy's address.
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            proxy_headers=True,
            forwarded_allow_ips=args.forwarded_allow_ips,
        )
    finally:
        _terminate_dashboard_watch(watch_proc)


if __name__ == "__main__":
    main()
