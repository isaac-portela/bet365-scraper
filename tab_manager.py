import re
import time
import json
import argparse
from typing import Dict, Optional, Set, Any

import requests
from websocket import create_connection

# ============================================================
# Regex para extrair C2 da URL da Bet365
# Ex: https://www.bet365.bet.br/#/IP/EV151292576782C1
# ============================================================
C2_RE = re.compile(r"/#/IP/EV15(\d+)2C1", re.IGNORECASE)

def extract_c2(url: str) -> Optional[str]:
    m = C2_RE.search(url or "")
    return m.group(1) if m else None


# ============================================================
# CDP CLIENT (Browser-level WebSocket)
# Usa Target.createTarget / Target.closeTarget
# ============================================================
class CDPClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.ws_url = self._get_browser_ws_url()
        self.ws = None
        self.next_id = 1

    def _get_browser_ws_url(self) -> str:
        r = requests.get(f"{self.base_url}/json/version", timeout=5)
        r.raise_for_status()
        data = r.json()
        ws = data.get("webSocketDebuggerUrl")
        if not ws:
            raise RuntimeError("webSocketDebuggerUrl não encontrado. Brave foi iniciado com --remote-debugging-port?")
        return ws

    def _ensure_ws(self):
        if self.ws is None:
            self.ws = create_connection(self.ws_url, timeout=10)

    def _call(self, method: str, params: dict | None = None) -> dict:
        self._ensure_ws()
        msg_id = self.next_id
        self.next_id += 1

        payload = {
            "id": msg_id,
            "method": method
        }
        if params:
            payload["params"] = params

        self.ws.send(json.dumps(payload))

        while True:
            resp = json.loads(self.ws.recv())
            if resp.get("id") == msg_id:
                if "error" in resp:
                    raise RuntimeError(f"CDP error {method}: {resp['error']}")
                return resp.get("result", {})

    def new_tab(self, url: str) -> dict:
        res = self._call("Target.createTarget", {"url": url})
        return {"id": res.get("targetId")}

    def close_tab(self, target_id: str) -> bool:
        self._call("Target.closeTarget", {"targetId": target_id})
        return True


# ============================================================
# LOCAL API HELPERS
# ============================================================
def get_targets(local_api: str, limit: int) -> list[str]:
    r = requests.get(
        f"{local_api}/targets",
        params={"limit": str(limit)},
        timeout=10
    )
    r.raise_for_status()
    data = r.json()
    return data.get("urls", []) if isinstance(data, dict) else []

def get_active_ids(local_api: str) -> Set[str]:
    r = requests.get(f"{local_api}/active_ids", timeout=10)
    r.raise_for_status()
    data = r.json()
    return {str(x) for x in data.get("ids", [])}


# ============================================================
# MAIN LOOP
# ============================================================
def run_loop(
    local_api: str,
    cdp_url: str,
    poll: float,
    max_tabs: int,
    open_per_tick: int
):
    cdp = CDPClient(cdp_url)

    # c2 -> { target_id, url, opened_at }
    open_tabs: Dict[str, Dict[str, Any]] = {}

    while True:
        start = time.time()

        # ----------------------------
        # Ativos
        # ----------------------------
        try:
            active_ids = get_active_ids(local_api)
        except Exception as e:
            print(f"[WARN] erro ao buscar active_ids: {e}")
            active_ids = set()

        # ----------------------------
        # Fechar abas inativas
        # ----------------------------
        for c2, meta in list(open_tabs.items()):
            if c2 not in active_ids:
                try:
                    cdp.close_tab(meta["target_id"])
                    print(f"[CLOSE] c2={c2} target={meta['target_id']}")
                except Exception as e:
                    print(f"[WARN] erro ao fechar c2={c2}: {e}")
                open_tabs.pop(c2, None)

        # ----------------------------
        # Abrir novas abas
        # ----------------------------
        remaining = max_tabs - len(open_tabs)
        if remaining > 0:
            try:
                urls = get_targets(local_api, min(open_per_tick, remaining))
            except Exception as e:
                print(f"[WARN] erro ao buscar targets: {e}")
                urls = []

            for url in urls:
                c2 = extract_c2(url)
                if not c2:
                    print(f"[SKIP] não consegui extrair c2: {url}")
                    continue

                if c2 in open_tabs:
                    continue

                if active_ids and c2 not in active_ids:
                    continue

                try:
                    created = cdp.new_tab(url)
                    tid = created.get("id")
                    if not tid:
                        print(f"[WARN] target sem id: {created}")
                        continue

                    open_tabs[c2] = {
                        "target_id": tid,
                        "url": url,
                        "opened_at": time.time()
                    }
                    print(f"[OPEN] c2={c2} target={tid}")

                except Exception as e:
                    print(f"[WARN] erro ao abrir {url}: {e}")

        # ----------------------------
        # Sleep
        # ----------------------------
        elapsed = time.time() - start
        time.sleep(max(0, poll - elapsed))


# ============================================================
# ENTRYPOINT
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local-api", default="http://127.0.0.1:8485")
    ap.add_argument("--cdp", default="http://127.0.0.1:9222")
    ap.add_argument("--poll", type=float, default=5.0)
    ap.add_argument("--max-tabs", type=int, default=12)
    ap.add_argument("--open-per-tick", type=int, default=3)

    args = ap.parse_args()

    print("=== Tab Manager ===")
    print(f"local_api: {args.local_api}")
    print(f"cdp:       {args.cdp}")
    print(f"poll:      {args.poll}s")
    print(f"max_tabs:  {args.max_tabs}")
    print(f"open/tick: {args.open_per_tick}")
    print("===================")

    run_loop(
        local_api=args.local_api.rstrip("/"),
        cdp_url=args.cdp.rstrip("/"),
        poll=args.poll,
        max_tabs=args.max_tabs,
        open_per_tick=args.open_per_tick
    )


if __name__ == "__main__":
    main()
