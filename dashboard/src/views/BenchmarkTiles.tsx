import React from "react";
import { Tag } from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";
import { apiUrl } from "../lib/api";

interface Props {
  items: BenchmarkSummary[];
  onSelect: (benchmarkId: string) => void;
  onEdit: (benchmarkId: string) => void;
  onAddNew: () => void;
}

export const BenchmarkTiles: React.FC<Props> = ({
  items,
  onSelect,
  onEdit,
  onAddNew,
}) => {
  const defaultLogoSrc = apiUrl("/api/static/benchmarks/logos/generic.png");

  const getLogoSrc = (logoFilename?: string | null): string => {
    if (!logoFilename) return defaultLogoSrc;
    return apiUrl(`/api/static/benchmarks/logos/${encodeURIComponent(logoFilename)}`);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))",
          gap: "0.75rem",
        }}
      >
        {items.map((item) => (
          <div
            key={item.benchmark_id}
            onClick={() => onSelect(item.benchmark_id)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onSelect(item.benchmark_id);
              }
            }}
            role="button"
            tabIndex={0}
            style={{
              border: "1px solid #c6c6c6",
              borderRadius: "8px",
              textAlign: "center",
              padding: "0.9rem",
              background: "#ffffff",
              cursor: "pointer",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: "0.55rem",
              minHeight: "180px",
              position: "relative",
            }}
          >
            <button
              type="button"
              aria-label={`Edit ${item.benchmark_id}`}
              title="Edit benchmark"
              onClick={(e) => {
                e.stopPropagation();
                onEdit(item.benchmark_id);
              }}
              style={{
                position: "absolute",
                top: "0.45rem",
                right: "0.45rem",
                width: "1.65rem",
                height: "1.65rem",
                border: "1px solid #c6c6c6",
                borderRadius: "999px",
                background: "#f4f4f4",
                cursor: "pointer",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "0.8rem",
                lineHeight: 1,
              }}
            >
              ✎
            </button>

            <div style={{ minWidth: 0, width: "100%" }}>
              <div
                style={{
                  margin: 0,
                  fontWeight: 600,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={item.name || item.benchmark_id}
              >
                {item.name || item.benchmark_id}
              </div>
              <div style={{ fontSize: "0.8rem", opacity: 0.8 }}>{item.benchmark_id}</div>
            </div>

            <img
              src={getLogoSrc(item.logo)}
              alt={`${item.name || item.benchmark_id} logo`}
              style={{ width: "82px", height: "82px", objectFit: "contain", borderRadius: "6px" }}
            />

            <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", justifyContent: "center" }}>
              <Tag type="blue">{item.db_type}</Tag>
              <Tag type="purple">{`${item.num_records} records`}</Tag>
            </div>

            <div
              style={{
                marginTop: "auto",
                fontSize: "0.85rem",
                opacity: 0.86,
                lineHeight: 1.35,
                textAlign: "center",
                overflow: "hidden",
                display: "-webkit-box",
                WebkitLineClamp: 3,
                WebkitBoxOrient: "vertical",
              }}
              title={item.description}
            >
              {item.description || "No description"}
            </div>
          </div>
        ))}

        <button
          type="button"
          onClick={onAddNew}
          style={{
            border: "1px dashed #6f6f6f",
            borderRadius: "8px",
            textAlign: "center",
            padding: "0.9rem",
            background: "rgba(255,255,255,0.35)",
            cursor: "pointer",
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            alignItems: "center",
            gap: "0.4rem",
            minHeight: "180px",
            fontWeight: 600,
          }}
        >
          <span style={{ fontSize: "1.4rem", lineHeight: 1 }}>+</span>
          Add New Benchmark
        </button>
      </div>
    </div>
  );
};
