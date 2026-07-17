"""HTTP intake. Thin by design - the API's only jobs are to say things
precisely and to never lie about idempotency:

    201  created, you'll want to poll it
    200  replay of an identical request; body is current status,
         X-Idempotent-Replay: true
    409  same operation_id, DIFFERENT payload - a client bug we refuse
         to paper over
    422  the payload never stood a chance

Run standalone for poking around:  python -m sluice.api
"""

from __future__ import annotations

import uuid

from flask import Flask, jsonify, request

from .config import Config
from .service import PayloadMismatch, SubmitResult, ValidationError, submit_withdrawal
from .store import Store


def create_app(store: Store, cfg: Config) -> Flask:
    app = Flask("sluice")

    @app.post("/v1/withdrawals")
    def submit():
        payload = request.get_json(silent=True)
        if payload is None:
            return jsonify(error={"code": "bad_json", "message": "body must be JSON"}), 400
        try:
            result: SubmitResult = submit_withdrawal(store, cfg, payload)
        except ValidationError as e:
            return jsonify(error={"code": "invalid", "field": e.field, "message": e.message}), 422
        except PayloadMismatch as e:
            return jsonify(error={"code": "payload_mismatch", "message": str(e)}), 409

        body = result.operation.public_view()
        if result.created:
            return jsonify(body), 201
        resp = jsonify(body)
        resp.headers["X-Idempotent-Replay"] = "true"
        return resp, 200

    @app.get("/v1/withdrawals/<op_id>")
    def status(op_id: str):
        try:
            oid = uuid.UUID(op_id)
        except ValueError:
            return jsonify(error={"code": "invalid", "message": "operation_id must be a UUID"}), 422
        op = store.get(oid)
        if op is None:
            return jsonify(error={"code": "not_found", "message": "unknown operation"}), 404
        return jsonify(op.public_view()), 200

    @app.get("/healthz")
    def healthz():
        return jsonify(ok=True), 200

    return app


if __name__ == "__main__":
    # Dev harness: in-memory store, no workers. Enough to poke the intake
    # contract with curl; the full lifecycle needs a worker (see README).
    from .store.memory import MemoryStore

    cfg = Config.from_env()
    create_app(MemoryStore(cfg.clock), cfg).run(port=8080, debug=False)
