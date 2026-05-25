# Match-Trader Position-Opened Bridge

Streams `PositionsServiceExternal.getOpenPositionsStreamByGroupsOrLogins` from Match-Trader, filters to `request_update_type == NEW` for a fixed list of logins, and POSTs each new position to an n8n webhook.

## Files

```
mt_bridge/
├── match_trader_grpc_bridge.py   # the service
├── proto/broker_api.proto        # gRPC schema
├── build.sh                      # regenerates stubs on deploy
├── requirements.txt
├── Procfile
├── railway.toml
├── .env.example
└── .gitignore
```

## Local test

```bash
cd mt_bridge
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
bash build.sh                          # generates broker_api_pb2{,_grpc}.py
cp .env.example .env                   # fill in real values
set -a; source .env; set +a
python match_trader_grpc_bridge.py
```

Open a position on one of the watchlisted accounts. Expected:

```
subscribing: 2 logins on grpc-broker-api-demo.match-trader.com:443
NEW position login=333376259 symbol=EURUSD side=BUY vol=0.1 @ 1.0823
```

## Railway deployment

1. `git init && git add . && git commit -m "initial"` → push to GitHub
2. Railway → New Project → Deploy from GitHub repo
3. **Variables tab** → set all values from `.env.example`
4. Deploy. Tail logs to confirm subscribe line.

## n8n workflow

**Webhook trigger**
- Method: POST
- Path: `/mt-position-opened`
- Authentication: Header Auth, header `X-Bridge-Token`, value matching `N8N_WEBHOOK_TOKEN`

**Code node**

```javascript
const p = $input.first().json;
const sideEmoji = p.side === 'BUY' ? '🟢' : '🔴';
const sl = p.stop_loss ? `\nSL: \`${p.stop_loss}\`` : '';
const tp = p.take_profit ? `\nTP: \`${p.take_profit}\`` : '';

return [{
  json: {
    chat_id: -100xxx,
    parse_mode: 'Markdown',
    text:
      `${sideEmoji} *Position opened*\n` +
      `Login: \`${p.login}\` (${p.group})\n` +
      `${p.symbol} ${p.side} ${p.volume}\n` +
      `Open: \`${p.open_price}\` @ ${p.open_time}${sl}${tp}\n` +
      `ID: \`${p.position_id}\``,
  },
}];
```

**Telegram node** — Send Message, wire fields from Code node.

## Webhook payload

```json
{
  "event": "position_opened",
  "login": "333376259",
  "group": "demo\\PTT\\Forex-20",
  "position_id": "POS-12345",
  "symbol": "EURUSD",
  "alias": "EURUSD",
  "side": "BUY",
  "volume": 0.1,
  "open_price": 1.0823,
  "open_time": "2026-05-25T08:15:32Z",
  "stop_loss": null,
  "take_profit": 1.0900,
  "commission": 0.0,
  "received_at": 1748172932
}
```

## Known gotchas

**Replay on connect.** Match-Trader may emit existing open positions as `NEW` on first subscribe and on every reconnect. Confirm behavior on first run; add dedupe later if needed.

**Demo vs prod endpoints.** `grpc-broker-api-demo.match-trader.com` is the demo host. Confirm prod host with Match-Trader support.

**Updating the watchlist.** Edit `MT_WATCHLIST_LOGINS` in Railway and redeploy. The stream will reconnect with the new list.
