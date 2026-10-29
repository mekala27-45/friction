import { site } from "@/lib/site";

export function Statement() {
  return (
    <p className="measure text-sm" style={{ color: "var(--ink2)" }} data-testid="statement">
      {site.statement}
    </p>
  );
}
