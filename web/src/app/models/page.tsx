import { DataTable } from "@/components/DataTable";
import { table, v } from "@/lib/site";

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
    </>
  );
}
