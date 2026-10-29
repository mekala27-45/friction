import { Pushback } from "@/components/Pushback";
import { site } from "@/lib/site";

const ICON: Record<string, string> = { critical: "▲", serious: "◆", warning: "●" };

export default function Operations() {
  const alerts = site.monitor?.alerts ?? [];
  return (
    <>
      <h1 className="text-3xl font-extrabold">Running it</h1>
      <p className="measure mt-3">The monitor after every scoring run: drift, expectation rates, completeness, the clock and label maturity. A breach fails the task and opens an issue with the runbook link.</p>
      <h2 className="mt-6 text-xl font-semibold">Alerts</h2>
      {alerts.length ? (
        <ul className="mt-2 grid gap-2">
          {alerts.map((a, i) => (
            <li key={i} className="slip p-3 text-sm">
              <span style={{ color: `var(--s-${a.severity})` }}>{ICON[a.severity] ?? ""} {a.severity}</span>{" "}
              <span className="font-semibold">{a.check}</span> <span className="num" style={{ textAlign: "left" }}>{a.scope}</span>: {a.note}
            </li>
          ))}
        </ul>
      ) : <p className="text-sm">The monitor has not run yet.</p>}
      <h2 className="mt-8 text-xl font-semibold">The day 40 incident</h2>
      <p className="measure text-sm">The planted fault: the previous day&apos;s file sent again, one part with a renamed column, one part with every clock six hours ahead. The postmortem is docs/incidents in the repository; the runbook is docs/runbooks/ingestion.md.</p>
      <Pushback who="a platform engineer">The incident ran on the build machine&apos;s copy of the pipeline until the workspace runs the daily job.</Pushback>
    </>
  );
}
