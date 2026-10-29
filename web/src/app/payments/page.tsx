import { DataTable } from "@/components/DataTable";
import { Pushback } from "@/components/Pushback";
import { site, table, v } from "@/lib/site";

export default function Payments() {
  const m = site.monitor?.maturity ?? [];
  return (
    <>
      <h1 className="text-3xl font-extrabold">The payments desk</h1>
      <p className="measure mt-3">
        Card transactions from TabFormer. Online purchases can be stepped up with a one time code; chip and swipe
        can only be approved or declined. Test months {v("plan.payments.test.start")} to {v("plan.payments.test.end")}:
        {" "}{v("eval.payments.test.events")} transactions and {v("eval.payments.test.frauds")} frauds worth
        {" "}{v("eval.payments.test.fraud_usd")}.
      </p>
      <DataTable t={table("eval.payments.scorecard")} caption="The model comparison" />
      <DataTable t={table("eval.payments.comparisons")} caption="Differences in dollar capture, Benjamini-Hochberg across the family" />
      <DataTable t={table("eval.payments.cost")} caption={`Expected cost per 1,000 events; cheapest: ${v("eval.payments.cost.cheapest")}`} />
      <p className="measure text-sm">Assumptions that change the cheapest model when halved or doubled: {v("eval.payments.cost.flips")}.</p>
      <h2 className="mt-8 text-xl font-semibold">Label maturity</h2>
      <p className="measure text-sm">Share of each month&apos;s frauds known within 30, 60 and 90 days of the event (simulated chargebacks).</p>
      {m.length ? (
        <div className="overflow-x-auto">
          <table className="dense">
            <thead><tr><th>Month</th><th className="num">Frauds</th><th className="num">30 days</th><th className="num">60 days</th><th className="num">90 days</th></tr></thead>
            <tbody>
              {m.slice(-12).map((r) => (
                <tr key={String(r.month)}>
                  <td>{String(r.month)}</td><td className="num">{String(r.frauds)}</td>
                  {["d30", "d60", "d90"].map((k) => <td key={k} className="num">{`${Math.round(Number(r[k]) * 100)}%`}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : <p className="text-sm">The monitor has not run yet.</p>}
      <DataTable t={table("eval.hypotheses")} caption="The registered hypotheses" />
      <Pushback who="a platform engineer">
        The labels are TabFormer&apos;s fraud flag delivered with a simulated lag, not chargebacks. The test months
        end where TabFormer stops marking fraud.
      </Pushback>
    </>
  );
}
