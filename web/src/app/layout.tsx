import type { Metadata } from "next";
import localFont from "next/font/local";
import { Nav } from "@/components/Nav";
import { Statement } from "@/components/Statement";
import "./globals.css";

const grotesk = localFont({
  variable: "--font-grotesk",
  display: "swap",
  src: [
    { path: "../../node_modules/@fontsource/schibsted-grotesk/files/schibsted-grotesk-latin-400-normal.woff2", weight: "400" },
    { path: "../../node_modules/@fontsource/schibsted-grotesk/files/schibsted-grotesk-latin-600-normal.woff2", weight: "600" },
    { path: "../../node_modules/@fontsource/schibsted-grotesk/files/schibsted-grotesk-latin-800-normal.woff2", weight: "800" },
  ],
});
const chivo = localFont({
  variable: "--font-chivo",
  display: "swap",
  src: [{ path: "../../node_modules/@fontsource/chivo-mono/files/chivo-mono-latin-400-normal.woff2", weight: "400" }],
});
const doto = localFont({
  variable: "--font-doto",
  display: "swap",
  src: [{ path: "../../node_modules/@fontsource/doto/files/doto-latin-700-normal.woff2", weight: "700" }],
});

export const metadata: Metadata = {
  title: "friction",
  description: "A fraud and identity risk lakehouse graded on what it costs good customers. Synthetic data only.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${grotesk.variable} ${chivo.variable} ${doto.variable}`}>
      <body className="font-sans antialiased">
        <div className="mx-auto max-w-5xl px-4 py-6">
          <header className="mb-6 flex flex-col gap-2 sm:flex-row sm:items-baseline sm:justify-between">
            <a href={process.env.NEXT_PUBLIC_BASE_PATH + "/"} className="text-2xl font-extrabold no-underline">friction</a>
            <Nav />
          </header>
          <main>{children}</main>
          <footer className="perf mt-12 pt-4">
            <Statement />
          </footer>
        </div>
      </body>
    </html>
  );
}
