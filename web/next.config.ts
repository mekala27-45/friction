import type { NextConfig } from "next";

// Static export for GitHub Pages under /friction. Link prefetch is off in the Nav component.
const base = process.env.FRICTION_BASE_PATH ?? "/friction";

const config: NextConfig = {
  output: "export",
  basePath: base,
  trailingSlash: true,
  images: { unoptimized: true },
  env: {
    NEXT_PUBLIC_FRICTION_API: process.env.NEXT_PUBLIC_FRICTION_API ?? "",
    NEXT_PUBLIC_BASE_PATH: base,
  },
};

export default config;
