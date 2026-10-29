import Link from "next/link";

const ROUTES: [string, string][] = [
  ["/", "Frontier"],
  ["/payments/", "Payments"],
  ["/logins/", "Logins"],
  ["/platform/", "Platform"],
  ["/governance/", "Governance"],
  ["/operations/", "Operations"],
  ["/replay/", "Replay"],
  ["/models/", "Models"],
  ["/report/", "Report"],
];

export function Nav() {
  return (
    <nav aria-label="Pages" className="flex flex-wrap gap-x-4 gap-y-1 text-sm">
      {ROUTES.map(([href, label]) => (
        <Link key={href} href={href} prefetch={false} className="no-underline hover:underline">
          {label}
        </Link>
      ))}
    </nav>
  );
}
