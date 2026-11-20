"""FastAPI ops console: inspect both databases, launch a settlement, recover one."""
from __future__ import annotations

from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from . import coordinator, db
from .config import (
    VALID_FAILPOINTS,
    InvalidSettlementNo,
    Settings,
    validate_settlement_no,
)

settings = Settings.from_env()
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_schema(settings.dsn_debit)
    db.init_schema(settings.dsn_credit)
    yield


app = FastAPI(title="Settlement 2PC Ops Console", lifespan=lifespan)


def _snapshot() -> dict:
    return {
        "debit": {
            "name": "扣款库 (debit)",
            "prepared": db.list_prepared(settings.dsn_debit),
        },
        "credit": {
            "name": "入账库 (credit)",
            "prepared": db.list_prepared(settings.dsn_credit),
        },
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {"failpoint": settings.failpoint,
         "failpoints": sorted(VALID_FAILPOINTS)},
    )


@app.get("/api/state")
def api_state():
    return JSONResponse({"failpoint": settings.failpoint, "databases": _snapshot()})


@app.get("/api/settlement/{settlement_no}")
def api_settlement(settlement_no: str):
    try:
        settlement_no = validate_settlement_no(settlement_no)
    except InvalidSettlementNo as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({
        "settlement_no": settlement_no,
        "debit_log": db.get_logs(settings.dsn_debit, settlement_no),
        "credit_log": db.get_logs(settings.dsn_credit, settlement_no),
        "debit_ledger": db.has_ledger_entry(settings.dsn_debit, settlement_no),
        "credit_ledger": db.has_ledger_entry(settings.dsn_credit, settlement_no),
        "gid": f"settle_{settlement_no}",
    })


@app.post("/api/settlement/start")
def api_start(settlement_no: str = Form(...), amount: float = Form(...)):
    try:
        settlement_no = validate_settlement_no(settlement_no)
    except InvalidSettlementNo as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    if amount < 0:
        return JSONResponse({"ok": False, "error": "金额不能为负"}, status_code=400)
    try:
        coordinator.start_settlement(
            settings.dsn_debit, settings.dsn_credit,
            settlement_no, amount, settings.failpoint)
    except coordinator.InconsistentSettlement as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    return JSONResponse({"ok": True, "settlement_no": settlement_no,
                         "note": "settlement driven to completion"})


@app.post("/api/settlement/recover")
def api_recover(settlement_no: str = Form(...)):
    try:
        settlement_no = validate_settlement_no(settlement_no)
    except InvalidSettlementNo as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    result = coordinator.recover_settlement(
        settings.dsn_debit, settings.dsn_credit, settlement_no)
    return JSONResponse({"ok": True, "result": result.__dict__})


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
