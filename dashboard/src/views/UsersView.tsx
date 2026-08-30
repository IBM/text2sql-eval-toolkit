import React, { useCallback, useEffect, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  Tag,
  TextInput,
} from "@carbon/react";

import { apiFetch, apiUrl } from "../lib/api";

/**
 * Granting and revoking roles.
 *
 * This replaced editing `TEXT2SQL_JUDGE_ALLOWLIST` in `deploy/.env` and
 * recreating the container.
 *
 * Two things the server tells us that the page has to show honestly. A grant
 * above the deployment's mode is *recorded but inert* -- the mode is a ceiling,
 * so granting `full` on a `judge` host stores the role and does nothing until
 * the ceiling is raised. Showing that as an ordinary grant is how someone
 * concludes the permission system is broken. And addresses from
 * `TEXT2SQL_ADMIN_EMAILS` cannot be edited here at all: they are the recovery
 * path for a deployment whose table is wrong, and are deliberately changeable
 * only with shell access.
 */

interface UserRow {
  email: string;
  role: string;
  effective_tier: string;
  active: boolean;
  inactive_reason: string | null;
  granted_by?: string | null;
  granted_at?: string | null;
}

interface UsersResponse {
  users: UserRow[];
  env_admins: string[];
  mode: string;
  roles: string[];
}

const ROLE_HELP: Record<string, string> = {
  read_only: "Browse published results. The default for anyone with no role.",
  judge: "Adds on-demand LLM-as-judge on a single record.",
  full: "Adds SQL execution, evaluation runs and registry writes.",
  admin: "Adds granting and revoking roles.",
};

export const UsersView: React.FC = () => {
  const [data, setData] = useState<UsersResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<string>("judge");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const res = await apiFetch(apiUrl("/api/users"));
      setData(await res.json());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load users");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const grant = async () => {
    const address = email.trim();
    if (!address) return;
    setBusy(true);
    setNotice(null);
    try {
      const res = await apiFetch(apiUrl("/api/users"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: address, role }),
      });
      const row: UserRow = await res.json();
      setEmail("");
      setNotice(
        row.active
          ? `${row.email} now has the ${row.role} role.`
          : `${row.email} was granted ${row.role}, but it is not active here. ${row.inactive_reason ?? ""}`,
      );
      await load();
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not grant the role");
    } finally {
      setBusy(false);
    }
  };

  const revoke = async (address: string) => {
    setBusy(true);
    setNotice(null);
    try {
      const res = await apiFetch(
        apiUrl(`/api/users/${encodeURIComponent(address)}`),
        {
          method: "DELETE",
        },
      );
      const body = await res.json();
      setNotice(
        body.still_admin_from_environment
          ? `${address} is named in TEXT2SQL_ADMIN_EMAILS and keeps admin. Remove it there to change that.`
          : `${address} is now read-only.`,
      );
      await load();
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not revoke the role");
    } finally {
      setBusy(false);
    }
  };

  const roles = data?.roles ?? ["read_only", "judge", "full", "admin"];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      <div>
        <h3 style={{ margin: 0 }}>Users</h3>
        <p
          style={{
            margin: "0.35rem 0 0",
            maxWidth: "52rem",
            lineHeight: 1.45,
            color: "var(--cds-text-secondary)",
          }}
        >
          Roles are matched against the verified email address the identity
          provider returns at sign-in, so an address must be exactly that —
          Gmail's dot and <code>+tag</code> variants reach the same inbox but
          are different strings. Everyone with no role is read-only.
        </p>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Users"
          subtitle={error}
          lowContrast
          onCloseButtonClick={() => setError(null)}
        />
      )}
      {notice && (
        <InlineNotification
          kind="info"
          title="Done"
          subtitle={notice}
          lowContrast
          onCloseButtonClick={() => setNotice(null)}
        />
      )}

      <section
        style={{
          border: "1px solid var(--cds-border-subtle-01)",
          borderRadius: 4,
          padding: "1rem",
          backgroundColor: "var(--cds-layer-01)",
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          alignItems: "flex-end",
        }}
      >
        <div style={{ flex: "2 1 18rem", minWidth: "min(100%, 14rem)" }}>
          <TextInput
            id="grant-email"
            labelText="Email address"
            placeholder="colleague@example.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
        </div>
        <div style={{ flex: "1 1 12rem", minWidth: "min(100%, 10rem)" }}>
          <ComboBox
            id="grant-role"
            titleText="Role"
            items={roles}
            itemToString={(item) => (item as string) ?? ""}
            selectedItem={role}
            onChange={({ selectedItem }) =>
              setRole((selectedItem as string) ?? "judge")
            }
            helperText={ROLE_HELP[role] ?? ""}
          />
        </div>
        <Button
          kind="primary"
          disabled={busy || !email.trim()}
          onClick={() => void grant()}
        >
          {busy ? "Working…" : "Grant"}
        </Button>
      </section>

      {data && (
        <p
          style={{
            margin: 0,
            fontSize: "0.875rem",
            color: "var(--cds-text-secondary)",
          }}
        >
          This deployment runs in <strong>{data.mode}</strong> mode, which is a
          ceiling: a role asking for more than that is stored but inactive until
          an operator raises it.
        </p>
      )}

      <TableContainer title="Granted roles">
        <Table size="sm">
          <TableHead>
            <TableRow>
              <TableHeader>Email</TableHeader>
              <TableHeader>Role</TableHeader>
              <TableHeader>Effective here</TableHeader>
              <TableHeader>Granted</TableHeader>
              <TableHeader> </TableHeader>
            </TableRow>
          </TableHead>
          <TableBody>
            {(data?.users ?? []).map((user) => (
              <TableRow key={user.email}>
                <TableCell
                  style={{ fontFamily: "monospace", fontSize: "0.8125rem" }}
                >
                  {user.email}
                </TableCell>
                <TableCell>
                  <Tag type="blue" size="sm">
                    {user.role}
                  </Tag>
                </TableCell>
                <TableCell>
                  {user.active ? (
                    <Tag type="green" size="sm">
                      {user.effective_tier}
                    </Tag>
                  ) : (
                    <span
                      style={{
                        display: "inline-flex",
                        flexDirection: "column",
                        gap: "0.2rem",
                      }}
                    >
                      <Tag type="gray" size="sm">
                        {user.effective_tier} — not active
                      </Tag>
                      <span
                        style={{
                          fontSize: "0.75rem",
                          color: "var(--cds-text-secondary)",
                          maxWidth: "24rem",
                          lineHeight: 1.4,
                        }}
                      >
                        {user.inactive_reason}
                      </span>
                    </span>
                  )}
                </TableCell>
                <TableCell style={{ fontSize: "0.8125rem" }}>
                  {user.granted_at ?? "—"}
                </TableCell>
                <TableCell>
                  <Button
                    kind="ghost"
                    size="sm"
                    disabled={busy}
                    onClick={() => void revoke(user.email)}
                  >
                    Revoke
                  </Button>
                </TableCell>
              </TableRow>
            ))}
            {data && data.users.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={5}
                  style={{ color: "var(--cds-text-secondary)" }}
                >
                  Nobody has been granted a role yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      {data && data.env_admins.length > 0 && (
        <section
          style={{ display: "flex", flexDirection: "column", gap: "0.4rem" }}
        >
          <h4 style={{ margin: 0, fontSize: "0.9375rem" }}>
            Administrators from the environment
          </h4>
          <p
            style={{
              margin: 0,
              fontSize: "0.8125rem",
              color: "var(--cds-text-secondary)",
              maxWidth: "48rem",
              lineHeight: 1.45,
            }}
          >
            Set in <code>TEXT2SQL_ADMIN_EMAILS</code> and always admin. They
            cannot be changed here on purpose: this is the way back into a
            deployment whose role table is wrong, so it takes shell access to
            alter.
          </p>
          <ul style={{ margin: 0, paddingLeft: "1.1rem" }}>
            {data.env_admins.map((address) => (
              <li
                key={address}
                style={{ fontFamily: "monospace", fontSize: "0.8125rem" }}
              >
                {address}
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
};
