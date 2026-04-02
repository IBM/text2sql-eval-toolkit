import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  InlineNotification,
  Modal,
  Select,
  SelectItem,
  TextArea,
  TextInput,
} from "@carbon/react";
import type { BenchmarkConfigInput } from "../types/benchmark";
import { apiUrl } from "../lib/api";

type Mode = "create" | "edit";

interface SubmitPayload {
  benchmark_id?: string;
  config: BenchmarkConfigInput;
}

interface Props {
  open: boolean;
  mode: Mode;
  benchmarkId: string | null;
  initialConfig: BenchmarkConfigInput | null;
  submitting: boolean;
  onClose: () => void;
  onSubmit: (payload: SubmitPayload) => Promise<void>;
  onUploadLogo: (file: File, benchmarkId: string) => Promise<string>;
}

const EMPTY_CONFIG: BenchmarkConfigInput = {
  name: "",
  description: "",
  data: "",
  schema: "",
  predictions: "",
  logo: "",
  db_engine: {
    db_type: "sqlite",
    db_folder: "",
    schema_name: "",
    connection_string_env_var: "",
  },
};

export const BenchmarkConfigModal: React.FC<Props> = ({
  open,
  mode,
  benchmarkId,
  initialConfig,
  submitting,
  onClose,
  onSubmit,
  onUploadLogo,
}) => {
  const [draftBenchmarkId, setDraftBenchmarkId] = useState("");
  const [draftConfig, setDraftConfig] = useState<BenchmarkConfigInput>(EMPTY_CONFIG);
  const [error, setError] = useState<string | null>(null);
  const [uploadingLogo, setUploadingLogo] = useState(false);
  const [selectedLogoFile, setSelectedLogoFile] = useState<File | null>(null);

  useEffect(() => {
    if (!open) return;
    setError(null);
    setSelectedLogoFile(null);
    setUploadingLogo(false);
    setDraftBenchmarkId(mode === "edit" ? benchmarkId || "" : "");
    if (initialConfig) {
      const legacyLogoUrl = (initialConfig as { logo_url?: string }).logo_url || "";
      const normalizedLogo =
        initialConfig.logo ||
        (legacyLogoUrl ? legacyLogoUrl.split("?")[0].split("/").pop() || "" : "");
      setDraftConfig({
        ...initialConfig,
        logo: normalizedLogo,
        db_engine: {
          db_type: initialConfig.db_engine?.db_type || "sqlite",
          db_folder: initialConfig.db_engine?.db_folder || "",
          schema_name: initialConfig.db_engine?.schema_name || "",
          connection_string_env_var:
            initialConfig.db_engine?.connection_string_env_var || "",
        },
      });
      return;
    }
    setDraftConfig(EMPTY_CONFIG);
  }, [benchmarkId, initialConfig, mode, open]);

  const dbType = draftConfig.db_engine.db_type || "sqlite";

  const logoPreview = useMemo(() => {
    if (selectedLogoFile) {
      return URL.createObjectURL(selectedLogoFile);
    }
    if (!draftConfig.logo) return "";
    return apiUrl(`/api/static/benchmarks/logos/${encodeURIComponent(draftConfig.logo)}`);
  }, [draftConfig.logo, selectedLogoFile]);

  useEffect(() => {
    return () => {
      if (logoPreview && logoPreview.startsWith("blob:")) {
        URL.revokeObjectURL(logoPreview);
      }
    };
  }, [logoPreview]);

  const updateEngineField = (key: string, value: string) => {
    setDraftConfig((prev) => ({
      ...prev,
      db_engine: {
        ...prev.db_engine,
        [key]: value,
      },
    }));
  };

  const submit = async () => {
    setError(null);
    const normalizedId = draftBenchmarkId.trim();
    if (mode === "create" && !normalizedId) {
      setError("Benchmark ID is required");
      return;
    }
    if (!draftConfig.name.trim()) {
      setError("Name is required");
      return;
    }
    if (!draftConfig.data.trim() || !draftConfig.schema.trim() || !draftConfig.predictions.trim()) {
      setError("Data, schema, and predictions paths are required");
      return;
    }
    if (dbType === "sqlite" && !(draftConfig.db_engine.db_folder || "").trim()) {
      setError("db_folder is required for sqlite");
      return;
    }
    if (
      dbType !== "sqlite" &&
      !(draftConfig.db_engine.connection_string_env_var || "").trim()
    ) {
      setError(`connection_string_env_var is required for ${dbType}`);
      return;
    }
    try {
      await onSubmit({
        benchmark_id: mode === "create" ? normalizedId : undefined,
        config: {
          ...draftConfig,
          logo: (draftConfig.logo || "").trim(),
          db_engine: {
            ...draftConfig.db_engine,
            db_type: dbType,
          },
        },
      });
    } catch (e) {
      const message = e instanceof Error ? e.message : "Failed to save benchmark";
      setError(message);
    }
  };

  const uploadLogo = async () => {
    if (!selectedLogoFile) return;
    const effectiveBenchmarkId = mode === "edit" ? (benchmarkId || "").trim() : draftBenchmarkId.trim();
    if (!effectiveBenchmarkId) {
      setError("Benchmark ID is required before uploading a logo");
      return;
    }
    setUploadingLogo(true);
    setError(null);
    try {
      const logoUrl = await onUploadLogo(selectedLogoFile, effectiveBenchmarkId);
      setDraftConfig((prev) => ({ ...prev, logo: logoUrl }));
      setSelectedLogoFile(null);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Failed to upload logo";
      setError(message);
    } finally {
      setUploadingLogo(false);
    }
  };

  return (
    <Modal
      open={open}
      modalHeading={mode === "create" ? "Add New Benchmark" : `Edit Benchmark: ${benchmarkId}`}
      primaryButtonText={mode === "create" ? "Create benchmark" : "Save changes"}
      secondaryButtonText="Cancel"
      primaryButtonDisabled={submitting || uploadingLogo}
      onRequestSubmit={() => {
        void submit();
      }}
      onRequestClose={onClose}
      size="lg"
    >
      <div style={{ display: "flex", flexDirection: "column", gap: "0.9rem" }}>
        {error ? (
          <InlineNotification
            kind="error"
            title="Unable to save benchmark"
            subtitle={error}
            lowContrast
          />
        ) : null}

        {mode === "create" ? (
          <TextInput
            id="benchmark-id-input"
            labelText="Benchmark ID"
            value={draftBenchmarkId}
            onChange={(e) => setDraftBenchmarkId(e.target.value)}
            placeholder="e.g. spider_dev"
          />
        ) : (
          <TextInput id="benchmark-id-readonly" labelText="Benchmark ID" readOnly value={benchmarkId || ""} />
        )}

        <TextInput
          id="benchmark-name-input"
          labelText="Name"
          value={draftConfig.name}
          onChange={(e) =>
            setDraftConfig((prev) => ({
              ...prev,
              name: e.target.value,
            }))
          }
        />

        <TextArea
          id="benchmark-description-input"
          labelText="Description"
          value={draftConfig.description}
          onChange={(e) =>
            setDraftConfig((prev) => ({
              ...prev,
              description: e.target.value,
            }))
          }
        />

        <Select
          id="benchmark-db-type"
          labelText="DB Type"
          value={dbType}
          onChange={(e) => updateEngineField("db_type", e.target.value)}
        >
          <SelectItem value="sqlite" text="sqlite" />
          <SelectItem value="postgres" text="postgres" />
          <SelectItem value="mysql" text="mysql" />
          <SelectItem value="db2" text="db2" />
          <SelectItem value="presto" text="presto" />
        </Select>

        {dbType === "sqlite" ? (
          <TextInput
            id="benchmark-db-folder"
            labelText="SQLite DB folder"
            value={draftConfig.db_engine.db_folder || ""}
            onChange={(e) => updateEngineField("db_folder", e.target.value)}
            placeholder="benchmarks/dbs/spider/database"
          />
        ) : (
          <TextInput
            id="benchmark-conn-env"
            labelText="Connection string env var"
            value={draftConfig.db_engine.connection_string_env_var || ""}
            onChange={(e) => updateEngineField("connection_string_env_var", e.target.value)}
            placeholder="POSTGRES_CONNECTION_STRING"
          />
        )}

        {(dbType === "postgres" || dbType === "db2") ? (
          <TextInput
            id="benchmark-schema-name"
            labelText="Schema name (optional)"
            value={draftConfig.db_engine.schema_name || ""}
            onChange={(e) => updateEngineField("schema_name", e.target.value)}
          />
        ) : null}

        <TextInput
          id="benchmark-data-path"
          labelText="Data path"
          value={draftConfig.data}
          onChange={(e) => setDraftConfig((prev) => ({ ...prev, data: e.target.value }))}
          placeholder="benchmarks/my_benchmark.json"
        />
        <TextInput
          id="benchmark-schema-path"
          labelText="Schema path"
          value={draftConfig.schema}
          onChange={(e) => setDraftConfig((prev) => ({ ...prev, schema: e.target.value }))}
          placeholder="benchmarks/my_benchmark-schema.json"
        />
        <TextInput
          id="benchmark-predictions-path"
          labelText="Predictions path"
          value={draftConfig.predictions}
          onChange={(e) => setDraftConfig((prev) => ({ ...prev, predictions: e.target.value }))}
          placeholder="results/my_benchmark-predictions.json"
        />

        <div style={{ display: "flex", flexDirection: "column", gap: "0.45rem" }}>
          <TextInput
            id="benchmark-logo-filename"
            labelText="Logo filename (optional)"
            value={draftConfig.logo || ""}
            onChange={(e) => setDraftConfig((prev) => ({ ...prev, logo: e.target.value }))}
            placeholder="my_benchmark.png"
          />
          <div style={{ display: "flex", alignItems: "center", gap: "0.45rem", flexWrap: "wrap" }}>
            <input
              type="file"
              accept="image/png,image/jpeg,image/webp,image/gif,image/svg+xml"
              onChange={(e) => setSelectedLogoFile(e.target.files?.[0] ?? null)}
            />
            <Button
              size="sm"
              kind="secondary"
              disabled={!selectedLogoFile || uploadingLogo || submitting}
              onClick={() => {
                void uploadLogo();
              }}
            >
              {uploadingLogo ? "Uploading..." : "Upload logo"}
            </Button>
            <Button
              size="sm"
              kind="ghost"
              disabled={uploadingLogo || submitting}
              onClick={() => {
                setSelectedLogoFile(null);
                setDraftConfig((prev) => ({ ...prev, logo: "" }));
              }}
            >
              Clear logo
            </Button>
          </div>
          {logoPreview ? (
            <img
              src={logoPreview}
              alt="Logo preview"
              style={{ width: "64px", height: "64px", objectFit: "contain", borderRadius: "4px" }}
            />
          ) : null}
        </div>
      </div>
    </Modal>
  );
};
