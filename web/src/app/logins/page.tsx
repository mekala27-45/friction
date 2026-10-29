import { DataTable } from "@/components/DataTable";
import { Pushback } from "@/components/Pushback";
import { table, v } from "@/lib/site";

export default function Logins() {
  return (
    <>
      <h1 className="text-3xl font-extrabold">The login desk</h1>
      <p className="measure mt-3">
        Sign ins from the RBA data set. A sign in is allowed, re-authenticated or blocked. The Freeman et al. (2016)
        model is written from the paper; LightGBM reads the same features. Real takeovers in the test months:
        {" "}{v("eval.logins.attackers.takeover.count")}, so the graded positives are the attacker models of the RBA
        papers, generated for held out users.
      </p>
      <DataTable t={table("eval.logins.scorecard")} caption="Freeman against LightGBM, test months" />
      <DataTable t={table("eval.logins.attackers")} caption="Capture by attacker model" />
      <DataTable t={table("eval.logins.rba_rsr")} caption="Risk score relation against the data set repository" />
      <DataTable t={table("eval.logins.rba_reauth")} caption="Median re-authentication rate against figure rq1-general" />
      <p className="measure text-sm">
        One account id holds {v("data.rba.unknown_account_attempts")} attempts with {v("data.rba.unknown_account_successes")} successes:
        the bucket for sign ins against unknown accounts. It is flagged by a rule on counts and never graded.
      </p>
      <Pushback who="a platform engineer">
        Takeover labels are rare and their intervals are wide. The comparison with the published results is against
        figures read from a repository, which notes its own stratification may differ.
      </Pushback>
    </>
  );
}
