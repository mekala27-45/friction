"""The scoring API: two score routes, the decision log, health, and the parity routes.

Every route a site visitor can call is counted against a budget of requests per hour per hashed
client address. The parity replay, run from GitHub Actions, carries the replay key and may name a
namespace of its own and seed it from a snapshot; nobody else can.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from friction_core.statements import STATEMENT
from pydantic import BaseModel

from friction_api import service
from friction_api.bundle import Bundle
from friction_api.service import DecisionOut, LoginIn, PaymentIn
from friction_api.store import Store

PUBLIC = "public"
BUDGET_PER_HOUR = 60


class SeedIn(BaseModel):
    namespace: str
    snapshot: dict[str, dict[str, Any]]


def _version() -> str:
    try:
        return version("friction-api")
    except PackageNotFoundError:
        return "0.0.0"


def client_hash(request: Request) -> str:
    """The client address, hashed with a key, so the budget table never holds an address."""
    forwarded = request.headers.get("x-forwarded-for", "")
    address = forwarded.split(",")[0].strip() or (request.client.host if request.client else "unknown")
    key = os.environ.get("FRICTION_CLIENT_KEY") or os.environ.get("FRICTION_SALT") or "friction-client"
    return hmac.new(key.encode(), address.encode(), hashlib.sha256).hexdigest()[:32]


def create_app(store: Store, bundles: dict[str, Bundle], replay_key: str | None = None,
               origins: list[str] | None = None, budget: int = BUDGET_PER_HOUR) -> FastAPI:  # fmt: skip
    app = FastAPI(title="friction", version=_version(), docs_url=None, redoc_url=None)
    if origins:
        app.add_middleware(
            CORSMiddleware, allow_origins=origins, allow_methods=["GET", "POST"], allow_headers=["content-type"]
        )

    def privileged(authorization: str | None) -> bool:
        if not replay_key or not authorization or not authorization.startswith("Bearer "):
            return False
        return hmac.compare_digest(authorization[7:].encode(), replay_key.encode())

    def gate(request: Request, authorization: str | None = Header(default=None),
             x_friction_namespace: str | None = Header(default=None)) -> str:  # fmt: skip
        if privileged(authorization):
            return x_friction_namespace or PUBLIC
        if x_friction_namespace not in (None, PUBLIC):
            raise HTTPException(status_code=403, detail="a namespace needs the replay key")
        allowed, _n = store.spend(client_hash(request), budget)
        if not allowed:
            raise HTTPException(
                status_code=429, detail=f"over {budget} requests this hour", headers={"Retry-After": "3600"}
            )
        return PUBLIC

    def require_key(authorization: str | None = Header(default=None)) -> None:
        if not privileged(authorization):
            raise HTTPException(status_code=403, detail="the replay key is required")

    def desk(name: str) -> Bundle:
        if name not in bundles:
            raise HTTPException(status_code=503, detail=f"no model is loaded for the {name} desk")
        return bundles[name]

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": _version(),
            "statement": STATEMENT,
            "models": {
                k: {
                    "model": b.model,
                    "status": b.status,
                    "bundle_digest": b.digest,
                    "files": b.files,
                    "thresholds": {"step_up": b.thresholds.step_up, "decline": b.thresholds.decline},
                }
                for k, b in sorted(bundles.items())
            },
        }

    @app.post("/v1/payments/score", response_model=DecisionOut)
    def score_payment(event: PaymentIn, namespace: str = Depends(gate)) -> DecisionOut:
        b = desk("payments")
        with store.transaction(namespace) as tx:
            return service.score_payment(tx, b, event)

    @app.post("/v1/logins/score", response_model=DecisionOut)
    def score_login(event: LoginIn, namespace: str = Depends(gate)) -> DecisionOut:
        b = desk("logins")
        with store.transaction(namespace) as tx:
            return service.score_login(tx, b, event)

    @app.get("/v1/decisions/{decision_id}")
    def decision(decision_id: str) -> dict[str, Any]:
        try:
            row = store.decision(decision_id)
        except Exception as exc:  # a malformed id is a not found, not a server error
            raise HTTPException(status_code=404, detail="no such decision") from exc
        if row is None:
            raise HTTPException(status_code=404, detail="no such decision")
        return row

    @app.post("/v1/admin/seed", dependencies=[Depends(require_key)])
    def seed(body: SeedIn) -> dict[str, Any]:
        if body.namespace == PUBLIC:
            raise HTTPException(status_code=400, detail="the public namespace is never reseeded")
        return {"namespace": body.namespace, "seeded": store.seed(body.namespace, body.snapshot)}

    @app.get("/v1/admin/state/{namespace}/{kind}/{key}", dependencies=[Depends(require_key)])
    def state(namespace: str, kind: str, key: str) -> dict[str, Any]:
        found = store.state(namespace, kind, key)
        if found is None:
            raise HTTPException(status_code=404, detail="no state")
        return found

    return app


def from_environment(loader: Callable[[Path], Bundle] = Bundle.load) -> FastAPI:
    # The fallback is the repository layout and is worked out only when FRICTION_MODELS is unset: in
    # the container the module sits three folders deep and a fourth parent does not exist.
    models = os.environ.get("FRICTION_MODELS")
    root = Path(models) if models else Path(__file__).resolve().parents[4] / "results" / "models"
    bundles = {p.parent.name: loader(p.parent) for p in sorted(root.glob("*/bundle.json"))}
    store = Store(os.environ["DATABASE_URL"])
    store.migrate()
    origins = [o for o in os.environ.get("FRICTION_CORS_ORIGINS", "").split(",") if o]
    return create_app(store, bundles, os.environ.get("FRICTION_REPLAY_KEY"), origins)
