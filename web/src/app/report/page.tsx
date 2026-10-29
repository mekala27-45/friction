import { DataTable } from "@/components/DataTable";
import { table, v } from "@/lib/site";

export default function Report() {
  return (
    <article className="measure" style={{ maxWidth: "68ch" }}>
      <h1 className="text-3xl font-extrabold">The briefing</h1>
      <p className="mt-3">
        {v("checklist.done")} of eighteen mandatory lines are done; not done: {v("checklist.missing")}. The graded
        numbers come from the {v("eval.source")} run.
      </p>
      <p className="mt-3">
        Payments: the champion is {v("eval.payments.champion")}. Logins: the champion is {v("eval.logins.champion")}.
        Both were chosen on validation by the registered gates, never on test.
      </p>
      <DataTable t={table("checklist")} caption="The definition of done" />
    </article>
  );
}
