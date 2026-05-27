"""
Match-Trader gRPC -> n8n webhook bridge.

Watches PositionsServiceExternal.getOpenPositionsStreamByGroupsOrLogins for a
fixed login watchlist, filters to request_update_type == NEW (newly opened
positions), and POSTs each event to an n8n webhook for Telegram delivery.

Deploy as a long-running Railway worker.

Env vars:
  MT_GRPC_HOST          grpc-broker-api-demo.match-trader.com:443
  MT_GRPC_USE_TLS       "true" / "false" (default true)
  MT_SYSTEM_UUID        broker system UUID
  MT_AUTH_TOKEN         bearer token from Match-Trader CRM
  MT_WATCHLIST_LOGINS   comma-separated logins, e.g. "333376259,1005,12345"
  N8N_WEBHOOK_URL       n8n webhook to POST NEW position events to
  N8N_WEBHOOK_TOKEN     shared secret sent as X-Bridge-Token (optional)
  PING_LOG_EVERY_S      log a liveness line every N seconds (default 300)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass

import grpc
import httpx

import broker_api_pb2 as pb
import broker_api_pb2_grpc as pb_grpc


# ---------- Config ----------

@dataclass(frozen=True)
class Config:
    host: str
    use_tls: bool
    system_uuid: str
    auth_token: str
    watchlist: frozenset[str]
    webhook_url: str
    webhook_token: str | None
    ping_log_every_s: int

    @classmethod
    def from_env(cls) -> "Config":
        logins_raw = os.environ.get("MT_WATCHLIST_LOGINS", "")
        watchlist = frozenset(
            login.strip() for login in logins_raw.split(",") if login.strip()
        )
        if not watchlist:
            raise RuntimeError("MT_WATCHLIST_LOGINS is empty - nothing to watch")
        return cls(
            host=os.environ["MT_GRPC_HOST"],
            use_tls=os.environ.get("MT_GRPC_USE_TLS", "true").lower() == "true",
            system_uuid=os.environ["MT_SYSTEM_UUID"],
            auth_token=os.environ["MT_AUTH_TOKEN"],
            watchlist=watchlist,
            webhook_url=os.environ["N8N_WEBHOOK_URL"],
            webhook_token=os.environ.get("N8N_WEBHOOK_TOKEN"),
            ping_log_every_s=int(os.environ.get("PING_LOG_EVERY_S", "300")),
        )


# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("mt-bridge")


# ---------- Webhook delivery ----------

async def post_to_n8n(client: httpx.AsyncClient, cfg: Config, payload: dict) -> None:
    """POST to n8n with bounded retry. Fire-and-forget so a slow webhook can't
    stall the gRPC stream."""
    headers = {"Content-Type": "application/json"}
    if cfg.webhook_token:
        headers["X-Bridge-Token"] = cfg.webhook_token

    backoff = 1.0
    for attempt in range(4):
        try:
            r = await client.post(cfg.webhook_url, json=payload, headers=headers, timeout=10.0)
            r.raise_for_status()
            return
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning(
                "webhook attempt %d failed: %s (login=%s position=%s)",
                attempt + 1, exc, payload.get("login"), payload.get("position_id"),
            )
            await asyncio.sleep(backoff)
            backoff *= 2
    log.error("webhook gave up (login=%s position=%s)",
              payload.get("login"), payload.get("position_id"))


# ---------- gRPC stream consumer ----------

def build_channel(cfg: Config) -> grpc.aio.Channel:
    options = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]
    if cfg.use_tls:
        return grpc.aio.secure_channel(cfg.host, grpc.ssl_channel_credentials(), options=options)
    return grpc.aio.insecure_channel(cfg.host, options=options)


def position_to_payload(login: str, pos: pb.PBPositionWithGroup) -> dict:
    return {
        "event": "position_opened" if pos.request_update_type == pb.NEW else "position_closed",
        "login": login,
        "group": pos.group,
        "position_id": pos.id,
        "symbol": pos.symbol,
        "alias": pos.alias,
        "side": pb.PBOrderSideExternal.Name(pos.side),
        "volume": pos.volume,
        "open_price": pos.open_price,
        "open_time": pos.open_time,
        "stop_loss": pos.stop_loss or None,
        "take_profit": pos.take_profit or None,
        "commission": pos.commission,
        "received_at": int(time.time()),
    }


async def consume_stream(cfg: Config, http: httpx.AsyncClient) -> None:
    """One pass over the stream. On any disconnect, raise so the outer loop reconnects."""
    async with build_channel(cfg) as channel:
        stub = pb_grpc.PositionsServiceExternalStub(channel)

        request = pb.PBOpenPositionRequestExternal(
            systemUuid=cfg.system_uuid,
            logins=list(cfg.watchlist),
        )
        metadata = [("authorization", f"Bearer {cfg.auth_token}")]

        log.info("subscribing: %d logins on %s", len(cfg.watchlist), cfg.host)
        last_log = time.monotonic()

        async for response in stub.getOpenPositionsStreamByGroupsOrLogins(
            request, metadata=metadata
        ):
            if response.HasField("heartbeat"):
                if time.monotonic() - last_log > cfg.ping_log_every_s:
                    log.info("stream alive (heartbeat)")
                    last_log = time.monotonic()
                continue

            positions = response.positionsByLogin.positionsByLogin
            for login, pos in positions.items():
                if login not in cfg.watchlist:
                    continue
                if pos.request_update_type not in (pb.NEW, pb.CLOSED):
                    continue

                payload = position_to_payload(login, pos)
 log.info(
    "%s login=%s id=%s symbol=%s side=%s vol=%s @ %s profit=%s",
    payload["event"], login, pos.id, pos.symbol, payload["side"],
    pos.volume, pos.open_price, pos.net_profit,
)
                asyncio.create_task(post_to_n8n(http, cfg, payload))


async def run_forever(cfg: Config) -> None:
    """Outer loop: reconnect with exponential backoff on any failure."""
    backoff = 1.0
    async with httpx.AsyncClient() as http:
        while True:
            try:
                await consume_stream(cfg, http)
                log.warning("stream ended cleanly; reconnecting")
                backoff = 1.0
            except grpc.aio.AioRpcError as exc:
                log.error("gRPC error: code=%s detail=%s", exc.code(), exc.details())
            except Exception as exc:  # noqa: BLE001
                log.exception("unexpected error: %s", exc)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


def main() -> None:
    cfg = Config.from_env()
    asyncio.run(run_forever(cfg))


if __name__ == "__main__":
    main()
