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
  PasswordInput,
  TextInput,
} from "@carbon/react";

import { apiFetch, apiUrl } from "../lib/api";

/**
 * Your own provider API keys.
 *
 * Deliberately write-only. The table shows that a key exists, its label and when
 * it was last used — never the key, not even masked. There is no endpoint that
 * could return one, so there is nothing here to render.
 *
 * Storing a key changes who *pays* for a request, not what you are allowed to
 * do. Capability comes from your role.
 */

interface StoredKey {
  provider: string;
  label: string | null;
  created_at: string;
  last_used_at: string | null;
}

export const MyKeysView: React.FC = () => {
  const [keys, setKeys] = useState<StoredKey[]>([]);
  const [providers, setProviders] = useState<string[]>([]);
  // Provider -> what its companion field is called, when it needs one. watsonx
  // takes a project id alongside its key, and a key without it is not a
  // credential: the server refuses to store half of one.
  const [secondaryLabels, setSecondaryLabels] = useState<
    Record<string, string>
  >({});
  const [secondary, setSecondary] = useState("");
  const [provider, setProvider] = useState("anthropic");
  const [apiKey, setApiKey] = useState("");
  const [label, setLabel] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const res = await apiFetch(apiUrl("/api/my/keys"));
      const body = await res.json();
      setKeys(body.keys ?? []);
      setProviders(body.providers ?? []);
      setSecondaryLabels(body.secondary_labels ?? {});
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load your keys");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const save = async () => {
    if (!apiKey.trim()) return;
    setBusy(true);
    setNotice(null);
    try {
      await apiFetch(apiUrl("/api/my/keys"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, api_key: apiKey, label, secondary }),
      });
      // Clear immediately: there is no reason for it to stay in the DOM, and no
      // way to get it back afterwards by design.
      setApiKey("");
      setLabel("");
      setSecondary("");
      setNotice(`Saved your ${provider} key. It cannot be displayed again.`);
      await load();
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not save the key");
    } finally {
      setBusy(false);
    }
  };

  const remove = async (which: string) => {
    setBusy(true);
    try {
      await apiFetch(apiUrl(`/api/my/keys/${encodeURIComponent(which)}`), {
        method: "DELETE",
      });
      setNotice(`Removed your ${which} key.`);
      await load();
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not remove the key");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      <div>
        <h3 style={{ margin: 0 }}>My API keys</h3>
        <p
          style={{
            margin: "0.35rem 0 0",
            maxWidth: "52rem",
            lineHeight: 1.45,
            color: "var(--cds-text-secondary)",
          }}
        >
          A stored key makes requests you run here bill your provider account
          instead of this server's. It does not change what you are allowed to
          do — that comes from your role. Keys are encrypted before they are
          stored and <strong>cannot be shown again</strong>, by you or anyone
          else; replace one by saving a new value.
        </p>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="API keys"
          subtitle={error}
          lowContrast
          onCloseButtonClick={() => setError(null)}
        />
      )}
      {notice && (
        <InlineNotification
          kind="success"
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
        <div style={{ flex: "1 1 10rem", minWidth: "min(100%, 9rem)" }}>
          <ComboBox
            id="key-provider"
            titleText="Provider"
            items={providers}
            itemToString={(item) => (item as string) ?? ""}
            selectedItem={provider}
            onChange={({ selectedItem }) => {
              setProvider((selectedItem as string) ?? "anthropic");
              setSecondary("");
            }}
          />
        </div>
        <div style={{ flex: "2 1 18rem", minWidth: "min(100%, 14rem)" }}>
          <PasswordInput
            id="key-value"
            labelText="API key"
            placeholder="Paste your key"
            value={apiKey}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setApiKey(e.target.value)
            }
            autoComplete="off"
          />
        </div>
        {secondaryLabels[provider] && (
          <div style={{ flex: "1 1 14rem", minWidth: "min(100%, 12rem)" }}>
            <TextInput
              id="key-secondary"
              labelText={secondaryLabels[provider]}
              placeholder="Required for this provider"
              value={secondary}
              onChange={(e) => setSecondary(e.target.value)}
              autoComplete="off"
            />
          </div>
        )}
        <div style={{ flex: "1 1 10rem", minWidth: "min(100%, 9rem)" }}>
          <TextInput
            id="key-label"
            labelText="Label (optional)"
            placeholder="personal"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
          />
        </div>
        <Button
          kind="primary"
          disabled={busy || !apiKey.trim()}
          onClick={() => void save()}
        >
          {busy ? "Saving…" : "Save"}
        </Button>
      </section>

      <TableContainer title="Stored keys">
        <Table size="sm">
          <TableHead>
            <TableRow>
              <TableHeader>Provider</TableHeader>
              <TableHeader>Label</TableHeader>
              <TableHeader>Added</TableHeader>
              <TableHeader>Last used</TableHeader>
              <TableHeader> </TableHeader>
            </TableRow>
          </TableHead>
          <TableBody>
            {keys.map((row) => (
              <TableRow key={row.provider}>
                <TableCell
                  style={{ fontFamily: "monospace", fontSize: "0.8125rem" }}
                >
                  {row.provider}
                </TableCell>
                <TableCell>{row.label || "—"}</TableCell>
                <TableCell style={{ fontSize: "0.8125rem" }}>
                  {row.created_at}
                </TableCell>
                <TableCell style={{ fontSize: "0.8125rem" }}>
                  {row.last_used_at ?? "never"}
                </TableCell>
                <TableCell>
                  <Button
                    kind="ghost"
                    size="sm"
                    disabled={busy}
                    onClick={() => void remove(row.provider)}
                  >
                    Remove
                  </Button>
                </TableCell>
              </TableRow>
            ))}
            {keys.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={5}
                  style={{ color: "var(--cds-text-secondary)" }}
                >
                  No keys stored. Requests use this server's credentials.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};
