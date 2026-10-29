import { DataTable } from "@/components/DataTable";
import { Pushback } from "@/components/Pushback";
import { table, v } from "@/lib/site";

const LAYERS: [string, string][] = [
  ["bronze", "every drop as it landed, text, with its file and rescued data"],
  ["silver", "typed, conformed, deduplicated in event time; the quarantine beside it"],
  ["cdc", "card history as SCD type 2; labels on two clocks"],
  ["gold", "point in time features, training sets as of a cut, scores and decisions"],
  ["ops", "run ledger, SLOs, drift, alerts and the compute ledger"],
];

export default function Platform() {
  return (
    <>
      <h1 className="text-3xl font-extrabold">The lakehouse</h1>
      <ol className="measure mt-4 grid gap-2 sm:grid-cols-5">
        {LAYERS.map(([k, d]) => (
          <li key={k} className="slip p-3 text-sm"><div className="font-semibold">{k}</div><div style={{ color: "var(--ink2)" }}>{d}</div></li>
        ))}
      </ol>
      <DataTable t={table("lake.local.tables")} caption="Every table of the full local run, with its digest" />
      <DataTable t={table("lake.local.quarantine")} caption={`Quarantine by rule, ${v("lake.local.quarantine_rows")} violations`} />
      <DataTable t={table("workspace.probe")} caption="What Free Edition allowed the API" />
      <Pushback who="a platform engineer">
        A file landing zone read with Auto Loader is not a message bus. Free Edition hides the cloud account, so no
        bucket, role or key owned by the build is shown. One workspace with prefixed schemas stands in for two.
      </Pushback>
    </>
  );
}
