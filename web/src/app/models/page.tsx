import { DataTable } from "@/components/DataTable";
import { has, table, v } from "@/lib/site";

export default function Models() {
  return (
    <>
      <h1 className="text-3xl font-extrabold">Model cards and validation reports</h1>
      <p className="measure mt-3">
        Each card and report renders from the manifest and refuses to render with a section missing. The full text
        is in docs/models and docs/validation in the repository. Champions: payments, {v("eval.payments.champion")};
        logins, {v("eval.logins.champion")}.
      </p>
      <DataTable t={table("eval.payments.gates")} caption="Payments: promotion gates" />
      <DataTable t={table("eval.logins.gates")} caption="Logins: promotion gates" />
      <DataTable t={table("eval.payments.interior_budget")} caption={`Payments: the interior test on the budget, ${v("eval.payments.interior.budget")}`} />
      <DataTable t={table("eval.logins.interior_budget")} caption={`Logins: the interior test on the budget, ${v("eval.logins.interior.budget")}`} />
      {has("v2.challengers") ? (
        <>
          <h2 className="mt-8 text-xl font-bold">Challengers after v0.1.0</h2>
          <p className="measure mt-2">
            Graph features and a sequence model, each registered in an addendum to the plan before any of its scores
            existed, trained on its champion&apos;s rows and promoted only with an interval above the champion.
          </p>
          <DataTable t={table("v2.challengers")} caption="Challengers against their champions, test months" />
        </>
      ) : null}
    </>
  );
}
