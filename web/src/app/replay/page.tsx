import { Replay } from "./Replay";
import { v } from "@/lib/site";

export default function ReplayPage() {
  return (
    <>
      <h1 className="text-3xl font-extrabold">Live</h1>
      <p className="measure mt-3">
        Send events from the parity replay set to the scoring API in order and watch the slips print. When the API
        is asleep the page plays the recorded session, the lakehouse&apos;s own decisions, and says so. Parity on the
        full set: {v("parity.local.payments.equal")} of {v("parity.local.payments.compared")} payment decisions and
        {" "}{v("parity.local.logins.equal")} of {v("parity.local.logins.compared")} login decisions equal in the local check.
      </p>
      <Replay />
    </>
  );
}
