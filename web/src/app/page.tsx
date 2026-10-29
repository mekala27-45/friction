import { DataTable } from "@/components/DataTable";
import { Frontier } from "@/components/Frontier";
import { Pushback } from "@/components/Pushback";
import { site, table, v } from "@/lib/site";

export default function Home() {
  const f = site.frontier;
  return (
    <>
      <h1 className="text-4xl font-extrabold leading-tight">What fraud capture costs good customers</h1>
      <p className="measure mt-3">
        Two desks, each with a floor of first day rules and models, graded on test months at a friction budget
        registered before those months were opened. Every number is a point on a frontier. Source of the graded
        numbers: {v("eval.source")} run.
      </p>
      {f?.payments ? (
        <Frontier desk={f.payments} unit="good transactions stepped up or declined per 1,000"
          title={`Payments: the champion, ${v("eval.payments.champion")}, captures ${v("eval.payments." + (f.payments.champion ?? "floor") + ".capture_usd.estimate")} of fraud dollars at the budget`} />
      ) : (
        <p className="mt-6">The payments desk has not been graded yet.</p>
      )}
      <DataTable t={table("eval.payments.scorecard")} caption="Payments, test months, at the operating point chosen on validation" />
      {f?.logins ? (
        <Frontier desk={f.logins} unit="legitimate sign ins re-authenticated or blocked per 1,000"
          title={`Logins: the champion, ${v("eval.logins.champion")}, captures ${v("eval.logins." + (f.logins.champion ?? "floor") + ".capture_usd.estimate")} of attacks at the budget`} />
      ) : (
        <p className="mt-6">The login desk has not been graded yet.</p>
      )}
      <DataTable t={table("eval.logins.scorecard")} caption="Logins, test months" />
      <Pushback who="a fraud lead">
        The cost parameters are assumptions. The attacker models come from the paper and are not attackers. The
        attack IP flag is close to a label, so the model that reads it is shown beside the one that does not. A
        synthetic window of card behavior is not a real portfolio.
      </Pushback>
    </>
  );
}
