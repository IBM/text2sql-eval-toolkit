import React, { useMemo, useState } from "react";
import {
  DataTable,
  DataTableHeader,
  DataTableSkeleton,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  TextInput,
} from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";

interface Props {
  items: BenchmarkSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export const BenchmarkList: React.FC<Props> = ({ items, selectedId, onSelect }) => {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const headers: DataTableHeader[] = [
    { key: "benchmark_id", header: "Benchmark" },
    { key: "description", header: "Description" },
    { key: "db_type", header: "DB Type" },
    { key: "num_records", header: "# Records" },
    { key: "num_pipelines", header: "# Pipelines" },
  ];

  const filtered = useMemo(
    () =>
      items.filter(
        (b) =>
          b.benchmark_id.toLowerCase().includes(search.toLowerCase()) ||
          b.description.toLowerCase().includes(search.toLowerCase())
      ),
    [items, search]
  );

  const total = filtered.length;
  const start = (page - 1) * pageSize;
  const end = start + pageSize;
  const pageItems = filtered.slice(start, end);
  const rows = pageItems.map((b) => ({
    id: b.benchmark_id,
    benchmark_id: b.name ? `${b.name} (${b.benchmark_id})` : b.benchmark_id,
    description: b.description,
    db_type: b.db_type,
    num_records: b.num_records,
    num_pipelines: b.num_pipelines,
  }));

  if (!items) {
    return <DataTableSkeleton />;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <TextInput
        id="benchmark-search"
        labelText="Search benchmarks"
        placeholder="Filter by id or description"
        value={search}
        onChange={(e) => {
          setSearch(e.target.value);
          setPage(1);
        }}
      />
      <div style={{ maxHeight: "480px", overflow: "auto" }}>
        <DataTable rows={rows} headers={headers} size="sm">
          {({ rows, headers, getHeaderProps, getRowProps }) => (
            <TableContainer>
              <Table aria-label="Benchmarks">
                <TableHead>
                  <TableRow>
                    {headers.map((header) => (
                      <TableHeader key={header.key} {...getHeaderProps({ header })}>
                        {header.header}
                      </TableHeader>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow
                      key={row.id}
                      {...getRowProps({ row })}
                      onClick={() => onSelect(row.id)}
                      style={{
                        cursor: "pointer",
                        backgroundColor:
                          selectedId === row.id ? "rgba(141, 192, 219, 0.3)" : undefined,
                      }}
                    >
                      {row.cells.map((cell) => (
                        <TableCell key={cell.id}>{cell.value}</TableCell>
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
        pageSizes={[10, 25, 50, 100]}
        totalItems={total}
        onChange={({ page, pageSize }) => {
          setPage(page);
          setPageSize(pageSize);
        }}
      />
    </div>
  );
};

