import { Pushback } from "@/components/Pushback";
import { site } from "@/lib/site";

// What a masked reader sees beside what the pipeline holds. The values are illustrative shapes, not
// rows: no address or token is published.
const MASKED: [string, string, string][] = [
  ["ip_key", "salted token of the address", "hidden"],
  ["ip_prefix16", "the /16 the address belongs to", "shown to readers"],
  ["ua_key", "salted token of the browser string", "hidden"],
  ["browser_family", "browser without its version", "shown to readers"],
  ["user_key", "salted token of the user", "shown to readers as the token"],
];

export default function Governance() {
  return (
    <>
      <h1 className="text-3xl font-extrabold">Unity Catalog</h1>
      <p className="measure mt-3">Classification by column, from the contracts. Restricted columns are masked for readers; sensitive ones are coarsened.</p>
      <div className="overflow-x-auto">
        <table className="dense mt-4" data-testid="classification">
          <thead><tr><th>Table</th><th>Column</th><th>Class</th><th>Meaning</th></tr></thead>
          <tbody>
            {site.classification.map((r) => (
              <tr key={r.table + r.column}><td>{r.table}</td><td className="num" style={{ textAlign: "left" }}>{r.column}</td><td>{r.classification}</td><td>{r.says}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
      <h2 className="mt-8 text-xl font-semibold">A masked and an unmasked read</h2>
      <div className="mt-2 grid gap-4 sm:grid-cols-2" data-testid="masked-read">
        <div className="slip p-3 text-sm"><div className="font-semibold">pipeline identity</div>
          <ul>{MASKED.map(([c, d]) => <li key={c}><span className="num">{c}</span>: {d}</li>)}</ul></div>
        <div className="slip p-3 text-sm"><div className="font-semibold">reader identity</div>
          <ul>{MASKED.map(([c, , r]) => <li key={c}><span className="num">{c}</span>: {r}</li>)}</ul></div>
      </div>
      <Pushback who="a platform engineer">
        The masks, grants and row filter are code in infra/terraform and the pipeline; until the workspace applies
        them, this page shows the contracts that define them, not a read from the workspace.
      </Pushback>
    </>
  );
}
