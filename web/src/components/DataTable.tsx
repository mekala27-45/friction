"use client";

import { useState } from "react";
import type { Table } from "@/lib/site";

// A manifest table with its two toggles: the rows, or the query that produced them.
export function DataTable({ t, sql, caption }: { t: Table | null; sql?: string; caption?: string }) {
  const [view, setView] = useState<"table" | "sql">("table");
  if (!t) return <p className="text-sm" style={{ color: "var(--ink2)" }}>Not measured yet.</p>;
  return (
    <figure className="my-4">
      {caption ? <figcaption className="mb-2 font-medium">{caption}</figcaption> : null}
      <div className="no-print mb-2 flex gap-3 text-xs" role="group" aria-label="View">
        <button type="button" aria-pressed={view === "table"} onClick={() => setView("table")} className="underline">
          table
        </button>
        <button type="button" aria-pressed={view === "sql"} onClick={() => setView("sql")} className="underline">
          show the SQL
        </button>
      </div>
      {view === "table" ? (
        <div className="overflow-x-auto">
          <table className="dense">
            <thead>
              <tr>
                {t.columns.map((c, i) => (
                  <th key={c} className={t.align[i] === "right" ? "num" : ""} scope="col">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {t.rows.map((r, i) => (
                <tr key={i}>
                  {r.map((cell, j) => (
                    <td key={j} className={t.align[j] === "right" ? "num" : ""}>
                      {cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <pre className="overflow-x-auto text-xs" style={{ fontFamily: "var(--font-chivo)" }}>
          {sql ?? `-- ${t.origin}\n-- source: ${t.source}`}
        </pre>
      )}
      <p className="mt-1 text-xs" style={{ color: "var(--ink2)" }}>
        Source: {t.source}. {t.origin}.
      </p>
    </figure>
  );
}
