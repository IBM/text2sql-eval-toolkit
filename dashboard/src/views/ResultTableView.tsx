import React, { useMemo, useState } from "react";
import {
  DataTable,
  DataTableHeader,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
} from "@carbon/react";

/**
 * A query result rendered as a table.
 *
 * There were two copies of this, one per detail view, and they drifted: the
 * pipeline-detail copy gained pagination and the error-analysis copy did not.
 * So the same 86,502-row Beaver result was 10 DOM rows in one panel and 86,502
 * in the other -- 854,563 nodes and 858 MB of heap, to fill a scroll box that
 * shows eight at a time. One copy now, paginated.
 *
 * `totalRows` is the count the query actually returned, which is not always the
 * number of rows here: the server trims large results to a preview. A table
 * showing 200 of 86,502 rows without saying so is a table that misrepresents
 * the query, so the footer states it.
 */
export function normalizeTableData(raw: any): { columns: string[]; rows: any[] } {
  let value = raw;

  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch {
      return { columns: ["value"], rows: [{ value }] };
    }
  }

  // pandas orient='split'
  if (
    value &&
    typeof value === "object" &&
    Array.isArray(value.columns) &&
    Array.isArray(value.data)
  ) {
    const columns = value.columns.map((c: any) => String(c));
    const rows = value.data.map((row: any[], idx: number) => {
      const out: Record<string, any> = { id: `r-${idx}` };
      columns.forEach((c: string, i: number) => {
        out[c] = row?.[i];
      });
      return out;
    });
    return { columns, rows };
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return { columns: [], rows: [] };
    if (typeof value[0] === "object" && value[0] !== null && !Array.isArray(value[0])) {
      const columnSet = new Set<string>();
      value.forEach((v) => Object.keys(v).forEach((k) => columnSet.add(k)));
      const columns = Array.from(columnSet);
      const rows = value.map((v, idx) => ({ id: `r-${idx}`, ...v }));
      return { columns, rows };
    }
    const rows = value.map((v, idx) => ({ id: `r-${idx}`, value: v }));
    return { columns: ["value"], rows };
  }

  if (value && typeof value === "object") {
    return { columns: Object.keys(value), rows: [{ id: "r-0", ...value }] };
  }

  return { columns: ["value"], rows: [{ id: "r-0", value: String(value) }] };
}

export const ResultTableView: React.FC<{
  title: string;
  rawData: any;
  /** Rows the query returned, when the server sent only a preview of them. */
  totalRows?: number | null;
}> = ({ title, rawData, totalRows }) => {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const normalized = useMemo(() => normalizeTableData(rawData), [rawData]);
  const headers: DataTableHeader[] = normalized.columns.map((c) => ({
    key: c,
    header: c,
  }));

  const available = normalized.rows.length;
  const actual = totalRows ?? available;
  const start = (page - 1) * pageSize;
  const pageRows = normalized.rows.slice(start, start + pageSize);

  return (
    <section
      style={{
        border: "1px solid rgba(15,98,254,0.2)",
        borderRadius: "6px",
        padding: "0.6rem",
        background: "#ffffff",
      }}
    >
      <h4 style={{ margin: "0 0 0.5rem 0", color: "#0f62fe" }}>{title}</h4>
      {headers.length === 0 ? (
        <div style={{ opacity: 0.8 }}>No rows</div>
      ) : (
        <>
          <div style={{ maxHeight: "280px", overflow: "auto" }}>
            <DataTable rows={pageRows} headers={headers} size="sm">
              {({ rows, headers, getHeaderProps }) => (
                <TableContainer>
                  <Table aria-label={title}>
                    <TableHead>
                      <TableRow>
                        {headers.map((header) => {
                          // Carbon's prop getter returns its own `key`; spreading
                          // it would override the explicit one, and React 18 warns
                          // on a spread `key`. Take it out and pass it directly.
                          const { key, ...headerProps } = getHeaderProps({ header });
                          return (
                            <TableHeader key={key} {...headerProps}>
                              {header.header}
                            </TableHeader>
                          );
                        })}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {rows.map((row) => (
                        <TableRow key={row.id}>
                          {row.cells.map((cell) => (
                            <TableCell key={cell.id}>
                              {cell.value == null ? "NULL" : String(cell.value)}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </DataTable>
          </div>
          <Pagination
            page={page}
            pageSize={pageSize}
            pageSizes={[10, 25, 50]}
            totalItems={available}
            size="sm"
            onChange={({ page, pageSize }) => {
              setPage(page);
              setPageSize(pageSize);
            }}
          />
          {actual > available && (
            <div style={{ marginTop: "0.4rem", fontSize: "0.75rem", opacity: 0.75 }}>
              Previewing {available.toLocaleString()} of{" "}
              {actual.toLocaleString()} rows the query returned. Open the raw
              JSON for the whole result.
            </div>
          )}
        </>
      )}
    </section>
  );
};
