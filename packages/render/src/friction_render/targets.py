"""Which templates render to which documents.

Convention rather than a hand kept list: every ``*.j2`` under ``docs/templates`` renders to the same
relative path under the repository root, and every one under ``report/templates`` renders under
``report/``. A file whose name starts with an underscore is a partial and is only included.
"""

from __future__ import annotations

from pathlib import Path

from friction_render.render import Target

TEMPLATE_ROOTS: tuple[tuple[Path, Path], ...] = (
    (Path("docs/templates"), Path()),
    (Path("report/templates"), Path("report")),
)


def discover(root: Path) -> list[Target]:
    targets: list[Target] = []
    for base, prefix in TEMPLATE_ROOTS:
        folder = root / base
        if not folder.exists():
            continue
        for template in sorted(folder.rglob("*.j2")):
            rel = template.relative_to(folder)
            if any(part.startswith("_") for part in rel.parts):
                continue
            targets.append(Target(template=rel.as_posix(), output=(prefix / rel.with_suffix("")).as_posix()))
    names = [t.template for t in targets]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"template names repeat across roots: {duplicates}")
    return targets


def template_roots(root: Path) -> list[Path]:
    return [root / base for base, _ in TEMPLATE_ROOTS if (root / base).exists()]
