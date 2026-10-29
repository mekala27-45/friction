"""The claim gate. Re-render every document from the manifest and diff whole files.

python scripts/check_published_numbers.py          # check, exit 1 on drift
python scripts/check_published_numbers.py --write  # render and write
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.manifest import load as load_manifest
from friction_render.render import ClaimGateError, Renderer, hand_typed_numbers, load_allowed_literals
from friction_render.targets import discover, template_roots

ALLOWED = Path("docs/templates/_allowed_literals.txt")


def build(root: Path) -> Renderer:
    manifest_path = root / "results" / "manifest.json"
    if not manifest_path.exists():
        raise ClaimGateError("results/manifest.json does not exist; run `make manifest` first")
    return Renderer(root, load_manifest(manifest_path), discover(root), template_roots(root))


def typed_numbers(root: Path) -> list[str]:
    allowed = load_allowed_literals(root / ALLOWED) if (root / ALLOWED).exists() else []
    problems: list[str] = []
    for base in template_roots(root):
        for template in sorted(base.rglob("*.j2")):
            found = hand_typed_numbers(template.read_text(encoding="utf-8"), allowed)
            if found:
                problems.append(f"{template.relative_to(root)}: {sorted(set(found))[:8]}")
    return problems


def check(root: Path) -> int:
    renderer = build(root)
    typed = typed_numbers(root)
    if typed:
        raise ClaimGateError("numbers typed by hand in templates:\n" + "\n".join(typed))
    drifts = renderer.check_all()
    if drifts:
        raise ClaimGateError(
            f"{len(drifts)} documents differ from the manifest:\n" + "\n".join(d.diff[:3000] for d in drifts)
        )
    return len(renderer.targets)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.write:
        written = build(ROOT).write_all()
        print(f"claim gate: rendered {len(written)} documents from the manifest")
        return 0
    count = check(ROOT)
    print(f"claim gate: {count} documents match the manifest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
