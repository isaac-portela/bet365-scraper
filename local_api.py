import os
import re
import traceback
import atexit
from datetime import datetime, timezone
from collections import defaultdict, deque

from flask_cors import CORS
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)
CORS(app)

# ============================================================
# CONFIGURAÇÃO (idioma / sufixo do feed)
# ============================================================
idioma = "en"

if idioma == "cn":
    DATA = {"C1A_10_0": [], "C18A_10_0": []}
    SUFIXO = "_10_0"
elif idioma == "en":
    DATA = {"C1A_1_3": [], "C18A_1_3": []}
    SUFIXO = "_1_3"
elif idioma == "gr":
    DATA = {"C1A_20_0": [], "C18A_20_0": []}
    SUFIXO = "_20_0"
else:
    DATA = {"C1A_1_3": [], "C18A_1_3": []}
    SUFIXO = "_1_3"

# ============================================================
# DEBUG RÁPIDO (pra não ficar "cego")
# ============================================================
DEBUG_PRINT_RAW_LEN = os.environ.get("BET_DEBUG_PRINT_RAW_LEN", "1") == "1"
DEBUG_RAW_PREVIEW_MAX = int(os.environ.get("BET_DEBUG_RAW_PREVIEW_MAX", "20"))
DEBUG_RAW_PREVIEW_CHARS = int(os.environ.get("BET_DEBUG_RAW_PREVIEW_CHARS", "4000"))
ULTIMOS_RAW = deque(maxlen=DEBUG_RAW_PREVIEW_MAX)

# ============================================================
# LOG LEVE DO RAW (ligado)
# ============================================================
LOG_RAW_ATIVO = True
PASTA_LOG_RAW = os.environ.get("BET_RAW_LOG_DIR", "C:/workspace/bet365-scraper/")
ARQUIVO_LOG_RAW = os.path.join(PASTA_LOG_RAW, "raw_websocket.txt")
MAX_BYTES_LOG_RAW = int(os.environ.get("BET_RAW_LOG_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB
LIMITE_BUFFER_LOG_RAW = int(os.environ.get("BET_RAW_LOG_BUFFER_LIMIT", "50"))  # flush a cada N mensagens
_BUFFER_RAW = []


def _rotacionar_log_raw_se_precisar():
    try:
        if os.path.exists(ARQUIVO_LOG_RAW) and os.path.getsize(ARQUIVO_LOG_RAW) >= MAX_BYTES_LOG_RAW:
            antigo = os.path.join(PASTA_LOG_RAW, "raw_websocket.old.txt")
            try:
                if os.path.exists(antigo):
                    os.remove(antigo)
            except:
                pass
            os.replace(ARQUIVO_LOG_RAW, antigo)
    except:
        pass


def salvar_raw_websocket_leve(dado: str):
    if not LOG_RAW_ATIVO:
        return
    try:
        _BUFFER_RAW.append(dado)
        if len(_BUFFER_RAW) < LIMITE_BUFFER_LOG_RAW:
            return

        os.makedirs(PASTA_LOG_RAW, exist_ok=True)
        _rotacionar_log_raw_se_precisar()
        with open(ARQUIVO_LOG_RAW, "a", encoding="utf-8") as f:
            f.write("\n\n".join(_BUFFER_RAW))
            f.write("\n\n")
        _BUFFER_RAW.clear()
    except:
        _BUFFER_RAW.clear()


def flush_raw_buffer():
    global _BUFFER_RAW
    try:
        if not _BUFFER_RAW:
            return
        os.makedirs(PASTA_LOG_RAW, exist_ok=True)
        _rotacionar_log_raw_se_precisar()
        with open(ARQUIVO_LOG_RAW, "a", encoding="utf-8") as f:
            f.write("\n\n".join(_BUFFER_RAW))
            f.write("\n\n")
    except:
        pass
    finally:
        _BUFFER_RAW.clear()


atexit.register(flush_raw_buffer)

# ============================================================
# ÍNDICES / STORES
# ============================================================
EVENTO_C2_PARA_OI = {}  # C2 -> OI

# Contexto por FI (essencial para updates)
EVENTO_POR_FI = {}          # FI -> C2
NOME_EVENTO_POR_FI = {}     # FI -> nome_evento
MERCADO_ATUAL_POR_FI = {}   # FI -> key_mercado corrente (interno)
MARKET_META_POR_FI = {}     # FI -> {"market_id":..., "market_it":..., "market_name":...}

# Fallback por selection_id (resolve o problema do U|OD com FI "diferente")
SELECTION_ID_TO_C2 = {}     # selection_id -> c2

# Store novo por evento C2
DADOS_MERCADO_POR_EVENTO = {}  # c2 -> {"nome_evento":..., "mercados":{key->mk}}

# Log didático por evento (para /explicar_frame)
FRAME_LOG_POR_C2 = defaultdict(lambda: deque(maxlen=50))

# ============================================================
# MODO "PAREADO COM A BET"
# - suspenso só muda quando SU vem no feed
# - não suspende por gol, não suspende por sumiço, não suspende por heurística
# ============================================================
BET_SUSPEND_ONLY_BY_SU = os.environ.get("BET_SUSPEND_ONLY_BY_SU", "1") == "1"

# ============================================================
# GC opcional (só REMOVE, não marca suspenso)
# OFF por padrão: BET_STALE_ENABLED=1 para ligar
# ============================================================
STALE_ENABLED = os.environ.get("BET_STALE_ENABLED", "0") == "1"
STALE_REMOVE_AFTER_SEC = int(os.environ.get("BET_STALE_REMOVE_AFTER_SEC", "120"))

# ============================================================
# TARGETS (abrir jogos automaticamente)
# ============================================================
BET_BASE = "https://www.bet365.bet.br/#/IP/"
TARGET_PREFIXO = "EV15"
TARGET_SUFIXO = "2C1"

TEMPO_ESPERA_TARGETS_SEG = 60
ULTIMO_ENVIO_TARGET_TS = {}

# =========================
# CAPTURA AUTOMÁTICA DO "ANTES/DEPOIS" DO GOL (debug)
# =========================
CAPTURA_GOL_ATIVA = True
GOL_FRAMES_ANTES = int(os.environ.get("BET_GOL_FRAMES_ANTES", "40"))
GOL_FRAMES_DEPOIS = int(os.environ.get("BET_GOL_FRAMES_DEPOIS", "40"))
GOL_DUMP_DIR = os.environ.get("BET_GOL_DUMP_DIR", "C:/workspace/bet365-scraper/gol_dumps")

RAW_RING_BY_C2 = defaultdict(lambda: deque(maxlen=GOL_FRAMES_ANTES))
PENDING_GOAL = {}  # c2 -> {"after_left": int, "score_before": str, "score_after": str, "ts": int}
LAST_SCORE_BY_C2 = {}  # c2 -> "x-y"

# ============================================================
# TEMPO
# ============================================================
def ts_agora_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def tu_para_ts_utc(tu: str):
    if not tu:
        return None
    try:
        return int(datetime.strptime(tu, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp())
    except:
        return None


# ============================================================
# LIMPEZA / NORMALIZAÇÃO
# ============================================================
_RE_NUM_LINHA = re.compile(r"^\s*[+-]?\d+(\.\d+)?\s*$")

_RE_FI_INPLAY_KEY = re.compile(r"(?:^|[^0-9])OV(\d{6,})C(?:1A|18A|151A)_")
_RE_FI_IN_DELTAKEY = re.compile(r"OV(\d{6,})-(\d{6,})")
FI_INPLAY_TO_DELTA_FIS = defaultdict(set)


def extrair_fis_inplay_do_raw(raw: str):
    if not raw:
        return set()
    try:
        s = raw.replace("\x15", "").replace("\x14", "").replace("\x01", "").replace("\x08", "")
        return set(m.group(1) for m in _RE_FI_INPLAY_KEY.finditer(s))
    except:
        return set()


def extrair_fis_do_raw(raw: str):
    if not raw:
        return set()
    try:
        s = raw.replace("\x15", "").replace("\x14", "").replace("\x01", "").replace("\x08", "")
        return set(m.group(1) for m in _RE_FI_IN_DELTAKEY.finditer(s))
    except:
        return set()


def aprender_fi_para_c2_no_mesmo_frame(raw: str):
    """
    Aprende correlação entre FI do InPlay e FI usado nos deltas (OV<FI>-<SID>)
    quando aparecem no mesmo frame raw.
    """
    fis_inplay = extrair_fis_inplay_do_raw(raw)
    fis_delta = extrair_fis_do_raw(raw)
    if fis_inplay and fis_delta:
        for fi_i in fis_inplay:
            FI_INPLAY_TO_DELTA_FIS[fi_i].update(fis_delta)
    return {"fis_inplay": list(fis_inplay), "fis_delta": list(fis_delta)}


def _clean_str(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


def _clean_line(x):
    s = _clean_str(x)
    if s is None:
        return None
    s = s.replace(" ", "")
    return s if s != "" else None


def resolver_c2_por_fi(fi: str):
    """
    Resolve C2 para um FI:
    1) direto EVENTO_POR_FI[fi]
    2) se fi for inplay e tiver deltas associados: tenta EVENTO_POR_FI[fi_delta]
    3) inverso: se algum fi_inplay aponta para esse fi, tenta EVENTO_POR_FI[fi_inplay]
    """
    fi = _clean_str(fi)
    if not fi:
        return None

    c2 = EVENTO_POR_FI.get(fi)
    if c2:
        return c2

    cand = FI_INPLAY_TO_DELTA_FIS.get(fi)
    if cand:
        for fi2 in list(cand):
            c2 = EVENTO_POR_FI.get(fi2)
            if c2:
                return c2

    for fi_inplay, deltas in FI_INPLAY_TO_DELTA_FIS.items():
        if fi in deltas:
            c2 = EVENTO_POR_FI.get(fi_inplay)
            if c2:
                return c2

    return None


def _is_placeholder_selection(sel: dict) -> bool:
    try:
        sid = _clean_str(sel.get("selection_id"))
        od = _clean_str(sel.get("od_frac"))
        nome = _clean_str(sel.get("nome"))
        sit = _clean_str(sel.get("selection_it"))
        ha = _clean_str(sel.get("linha_ha"))
        hd = _clean_str(sel.get("linha_hd"))

        if sid or od:
            return False

        if nome and _RE_NUM_LINHA.match(nome) and (not ha) and (not hd):
            return True

        if sit and ("C1-" in sit) and (not od) and (not ha) and (not hd):
            if (not nome) or _RE_NUM_LINHA.match(nome or ""):
                return True

        return False
    except:
        return False


# ============================================================
# PARSE BÁSICO
# ============================================================
def parse_kv_ponto_virgula(txt: str) -> dict:
    d = {}
    try:
        if not txt:
            return d
        txt = txt.strip()

        if txt.startswith("|"):
            txt = txt[1:]
        if txt.endswith("|"):
            txt = txt[:-1]

        if txt.endswith(";"):
            txt = txt[:-1]
        if not txt:
            return d

        for item in txt.split(";"):
            if "=" in item:
                k, v = item.split("=", 1)
                d[k] = v
    except:
        return {}
    return d


def inserir_evento_na_lista(it, obj, chave_categoria):
    if it not in DATA[chave_categoria]:
        DATA[chave_categoria].append(it)
        if it not in DATA:
            DATA[it] = obj


def odds_para_decimal(od: str):
    if not od:
        return None
    od = od.strip().upper()
    if od in ("EVS", "EVENS"):
        return 2.0
    if od == "0/0":
        return None
    if "/" in od:
        try:
            a, b = od.split("/", 1)
            a = float(a)
            b = float(b)
            if b == 0:
                return None
            return round(1.0 + (a / b), 6)
        except:
            return None
    try:
        return float(od)
    except:
        return None


def indexar_ids_evento_se_existirem(d: dict):
    try:
        if not isinstance(d, dict):
            return
        c2 = str(d.get("C2", "")).strip()
        oi = str(d.get("OI", "")).strip()
        if c2 and oi:
            EVENTO_C2_PARA_OI[c2] = oi
    except:
        pass


# ============================================================
# STORE HELPERS
# ============================================================
def _garantir_store_evento(c2: str, nome_evento: str = None):
    if not c2:
        return None
    st = DADOS_MERCADO_POR_EVENTO.get(c2)
    if not st:
        st = {"nome_evento": nome_evento, "mercados": {}}
        DADOS_MERCADO_POR_EVENTO[c2] = st
    else:
        if nome_evento:
            st["nome_evento"] = nome_evento
    return st


def dump_goal_file(c2: str):
    try:
        os.makedirs(GOL_DUMP_DIR, exist_ok=True)
        pend = PENDING_GOAL.get(c2)
        if not pend:
            return

        sb = (pend.get("score_before") or "").replace(":", "-")
        sa = (pend.get("score_after") or "").replace(":", "-")
        ts = str(pend.get("ts") or ts_agora_utc())

        fname = f"goal_{c2}_{sb}_{sa}_{ts}.txt"
        path = os.path.join(GOL_DUMP_DIR, fname)

        buf = list(RAW_RING_BY_C2.get(c2, []))
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n\n".join(buf))
    except:
        pass


def _touch_market(mk: dict, now_ts: int):
    if isinstance(mk, dict):
        mk["_last_seen_ts"] = now_ts


def _touch_selection(sel: dict, now_ts: int):
    if isinstance(sel, dict):
        sel["_last_seen_ts"] = now_ts


# ============================================================
# REGEX: Goal Markets
# ============================================================
_RE_GOAL_MARKET = re.compile(r"\b(\d+(st|nd|rd|th)\s+Goal|Next\s+Goal|Goal)\b", re.IGNORECASE)


def is_goal_market(mk: dict) -> bool:
    try:
        nome = str(mk.get("nome_mercado") or "")
        return bool(_RE_GOAL_MARKET.search(nome))
    except:
        return False


def purge_goal_markets(c2: str, now_ts: int, reason: str = "goal_detected"):
    """
    QUANDO SAIR GOL:
    - NÃO suspende nada (pareado com bet)
    - remove os mercados de gol do store para não "ficar preso" no 3rd goal etc
    """
    store = DADOS_MERCADO_POR_EVENTO.get(c2)
    if not store:
        return

    mercados = store.get("mercados", {})
    if not isinstance(mercados, dict):
        return

    keys_rm = []
    removed = []
    for k, mk in list(mercados.items()):
        if isinstance(mk, dict) and is_goal_market(mk):
            keys_rm.append(k)

    for k in keys_rm:
        mk = mercados.pop(k, None)
        if mk:
            removed.append(k)

    if removed:
        FRAME_LOG_POR_C2[c2].append({
            "ts": now_ts,
            "summary": {"type": "PURGE_GOAL_MARKETS", "reason": reason, "removed": removed}
        })


# ============================================================
# NOVO: PRÓXIMO GOL (NTH GOAL) + ODDS OFICIAIS DA BET
# ============================================================
_RE_SCORE = re.compile(r"^\s*(\d+)\s*[-:]\s*(\d+)\s*$")


def _parse_total_goals_from_score(ss: str):
    """
    ss vem do EV.SS (ex: '6-5'). Retorna total (11) ou None se inválido.
    """
    if not ss:
        return None
    m = _RE_SCORE.match(str(ss))
    if not m:
        return None
    try:
        a = int(m.group(1))
        b = int(m.group(2))
        if a < 0 or b < 0:
            return None
        return a + b
    except:
        return None


def _ordinal_en(n: int) -> str:
    # 1st, 2nd, 3rd, 4th ... com exceção 11/12/13 -> th
    if n % 100 in (11, 12, 13):
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def _find_market_case_insensitive(mercados_by_name: dict, desired_name: str):
    """
    mercados_by_name é dict 'nome normalizado' -> mk
    Retorna (nome_encontrado, mk) ou (None, None)
    """
    if not isinstance(mercados_by_name, dict) or not desired_name:
        return (None, None)
    wanted = desired_name.strip().lower()
    for k, v in mercados_by_name.items():
        if str(k).strip().lower() == wanted:
            return (k, v)
    return (None, None)


def _find_next_goal_market(mercados_by_name: dict, next_n: int):
    """
    Procura primeiro o mercado exato "12th Goal" (exemplo),
    fallback para "Next Goal",
    e fallback final para variações que contenham o ordinal + 'goal'.
    """
    if not isinstance(mercados_by_name, dict) or not next_n:
        return {
            "expected_market": None,
            "market_name_found": None,
            "selecoes": [],
            "reason": "no_markets_or_invalid_next_n",
        }

    expected = f"{_ordinal_en(next_n)} Goal"  # ex: "12th Goal"

    # 1) match exato
    name_found, mk = _find_market_case_insensitive(mercados_by_name, expected)
    if mk:
        return {
            "expected_market": expected,
            "market_name_found": name_found,
            "selecoes": mk.get("selecoes", []) if isinstance(mk, dict) else [],
            "reason": "exact_match",
        }

    # 2) fallback para Next Goal (às vezes a Bet usa esse)
    name_found, mk = _find_market_case_insensitive(mercados_by_name, "Next Goal")
    if mk:
        return {
            "expected_market": expected,
            "market_name_found": name_found,
            "selecoes": mk.get("selecoes", []) if isinstance(mk, dict) else [],
            "reason": "fallback_next_goal",
        }

    # 3) fallback “contém”
    ord_lower = _ordinal_en(next_n).lower()
    for k, v in mercados_by_name.items():
        kl = str(k).lower()
        if (ord_lower in kl) and ("goal" in kl):
            return {
                "expected_market": expected,
                "market_name_found": k,
                "selecoes": v.get("selecoes", []) if isinstance(v, dict) else [],
                "reason": "contains_match",
            }

    return {
        "expected_market": expected,
        "market_name_found": None,
        "selecoes": [],
        "reason": "not_found_in_snapshot",
    }


# ============================================================
# UPDATES U|... (DELTA) PARA SELEÇÕES
# ============================================================
_RE_FI_SID = re.compile(r"(\d{6,})-(\d{6,})(?:_|$)")


def _extrair_fi_e_sid_da_chave(chave: str):
    try:
        if not chave:
            return (None, None)
        m = _RE_FI_SID.search(chave)
        if not m:
            return (None, None)
        fi, sid = m.group(1), m.group(2)
        return (fi, sid)
    except:
        return (None, None)


def _atualizar_selecao_no_evento_por_sid(c2: str, sid: str, patch: dict, now_ts: int) -> bool:
    try:
        store = DADOS_MERCADO_POR_EVENTO.get(c2)
        if not store:
            return False

        mercados = store.get("mercados", {})
        if not isinstance(mercados, dict):
            return False

        applied = False
        for mk in mercados.values():
            mp = mk.get("_selecoes_map", {})
            if not isinstance(mp, dict) or not mp:
                continue

            old = mp.get(sid)
            key_found = sid

            if not old:
                for k, v in mp.items():
                    try:
                        if str(v.get("selection_id") or "").strip() == sid:
                            old = v
                            key_found = k
                            break
                        itv = str(v.get("selection_it") or "")
                        if sid and (f"-{sid}" in itv):
                            old = v
                            key_found = k
                            break
                    except:
                        continue

            if not old:
                continue

            if "od_frac" in patch and _clean_str(patch.get("od_frac")):
                old["od_frac"] = _clean_str(patch["od_frac"])
                old["od_dec"] = odds_para_decimal(old["od_frac"])

            if "linha_ha" in patch and patch.get("linha_ha") is not None:
                old["linha_ha"] = _clean_line(patch.get("linha_ha"))

            if "linha_hd" in patch and patch.get("linha_hd") is not None:
                old["linha_hd"] = _clean_line(patch.get("linha_hd"))

            # PAREADO COM A BET: só altera suspenso se SU veio no delta
            if "suspenso" in patch and patch.get("suspenso") is not None:
                old["suspenso"] = bool(patch.get("suspenso"))

            if "ordem" in patch and _clean_str(patch.get("ordem")):
                old["ordem"] = _clean_str(patch.get("ordem"))

            if "nome" in patch and _clean_str(patch.get("nome")):
                old["nome"] = _clean_str(patch.get("nome"))

            if "n2" in patch and _clean_str(patch.get("n2")):
                old["n2"] = _clean_str(patch.get("n2"))

            old["selection_id"] = sid
            if _clean_str(patch.get("selection_it")):
                old["selection_it"] = _clean_str(patch.get("selection_it"))

            if key_found != sid:
                mp[sid] = old

            _touch_market(mk, now_ts)
            _touch_selection(old, now_ts)
            applied = True

        return applied
    except:
        return False


def aplicar_delta_mercados_u(chave: str, kv: dict, now_ts: int):
    """
    Delta U aplicado por chave tipo: OV<FI>-<SID>...
    IMPORTANTÍSSIMO:
    - suspenso só muda se SU existir no kv (pareado com bet)
    """
    try:
        if not chave or not isinstance(kv, dict) or not kv:
            return None

        fi, sid = _extrair_fi_e_sid_da_chave(chave)
        if not fi or not sid:
            return None

        c2 = resolver_c2_por_fi(fi)
        if not c2:
            c2 = SELECTION_ID_TO_C2.get(sid)
        if not c2:
            return None

        if fi:
            EVENTO_POR_FI[fi] = c2

        patch = {
            "selection_it": chave,
            "od_frac": kv.get("OD"),
            "linha_ha": kv.get("HA"),
            "linha_hd": kv.get("HD"),
            "ordem": kv.get("OR"),
            "nome": kv.get("NA"),
            "n2": kv.get("N2"),
        }

        if "SU" in kv:
            patch["suspenso"] = (str(kv.get("SU")).strip() == "1")

        if _atualizar_selecao_no_evento_por_sid(c2, sid, patch, now_ts):
            return c2

        # fallback: cria num mercado "FI:<fi>" (apenas pra não perder selection)
        store = _garantir_store_evento(c2, None)
        if not store:
            return c2

        key_mk = MERCADO_ATUAL_POR_FI.get(fi) if fi else None
        if not key_mk:
            key_mk = f"FI:{fi}"

        mk = store["mercados"].get(key_mk)
        if not mk:
            meta = MARKET_META_POR_FI.get(fi, {}) if fi else {}
            mk = {
                "nome_mercado": (meta.get("market_name") or key_mk),
                "market_id": _clean_str(meta.get("market_id")),
                "market_it": _clean_str(meta.get("market_it")),
                "suspenso": False,
                "_selecoes_map": {}
            }
            store["mercados"][key_mk] = mk

        mp = mk.get("_selecoes_map")
        if not isinstance(mp, dict):
            mp = {}
            mk["_selecoes_map"] = mp

        od_frac = _clean_str(patch.get("od_frac"))
        sel = {
            "nome": _clean_str(patch.get("nome")),
            "od_frac": od_frac,
            "od_dec": odds_para_decimal(od_frac) if od_frac else None,
            "linha_ha": _clean_line(patch.get("linha_ha")),
            "linha_hd": _clean_line(patch.get("linha_hd")),
            "selection_it": _clean_str(patch.get("selection_it")),
            "selection_id": sid,
            "suspenso": (bool(patch.get("suspenso")) if ("suspenso" in patch) else False),
            "ordem": _clean_str(patch.get("ordem")),
            "n2": _clean_str(patch.get("n2")),
        }

        mp[sid] = sel
        _touch_market(mk, now_ts)
        _touch_selection(sel, now_ts)
        return c2
    except:
        return None


# ============================================================
# PARSER DO AO VIVO (PLACAR / TEMPO / NOMES)
# ============================================================
def aplicar_delta_placar(target_key, txt, touched_c2: set, now_ts: int):
    acao, dados = txt.split("|", 1)

    if acao == "U":
        dados = (dados or "").strip().strip("|")
        dit = parse_kv_ponto_virgula(dados)

        # DETECTA GOL -> PURGE mercados de gol (não suspende nada)
        try:
            info_evt = DATA.get(target_key, {}) if isinstance(DATA.get(target_key), dict) else {}
            c2_evt = str(info_evt.get("C2", "")).strip()
            ss_before = (LAST_SCORE_BY_C2.get(c2_evt) or "").strip() if c2_evt else ""
            ss_now = str(info_evt.get("SS", "")).strip()
            uc = str(dit.get("UC", "")).strip()

            is_goal = False
            if uc.lower() == "goal":
                is_goal = True
            elif ss_before and ss_now and ss_now != ss_before:
                is_goal = True

            if c2_evt and ss_now:
                LAST_SCORE_BY_C2[c2_evt] = ss_now

            if c2_evt and is_goal:
                purge_goal_markets(c2_evt, now_ts, reason="score_change")

                if CAPTURA_GOL_ATIVA:
                    PENDING_GOAL[c2_evt] = {
                        "after_left": GOL_FRAMES_DEPOIS,
                        "score_before": ss_before,
                        "score_after": ss_now,
                        "ts": now_ts
                    }
        except:
            pass

        if isinstance(DATA.get(target_key), dict):
            DATA[target_key].update(dit)
            indexar_ids_evento_se_existirem(DATA[target_key])
        else:
            DATA[target_key] = dit
            indexar_ids_evento_se_existirem(dit)

        c2_aplicado = aplicar_delta_mercados_u(target_key, dit, now_ts)
        if c2_aplicado and isinstance(touched_c2, set):
            touched_c2.add(c2_aplicado)

    elif acao == "I":
        if len(dados) >= 3:
            tipo = dados[:2]
            corpo = dados[3:]
        else:
            tipo = ""
            corpo = dados

        dit = parse_kv_ponto_virgula(corpo)
        it = dit.get("IT")
        if not it:
            return

        if it not in DATA:
            DATA[it] = dit
        else:
            if isinstance(DATA[it], dict):
                DATA[it].update(dit)
            else:
                DATA[it] = dit

        indexar_ids_evento_se_existirem(DATA[it])

        if tipo == "EV":
            if "C1A_" in it:
                inserir_evento_na_lista(it, dit, f"C1A{SUFIXO}")
            elif "C18A_" in it:
                inserir_evento_na_lista(it, dit, f"C18A{SUFIXO}")

    elif acao == "D":
        it = target_key.split("/")[-1]
        for key in (f"C1A{SUFIXO}", f"C18A{SUFIXO}"):
            if it in DATA.get(key, []):
                DATA[key].remove(it)
        if it in DATA:
            del DATA[it]


def inicializar_snapshot_placar(txt):
    lst = txt.split("|")[1:]
    for item in lst:
        tipo = item[:2]
        obj = parse_kv_ponto_virgula(item[3:])
        it = obj.get("IT")
        if not it:
            continue

        if tipo == "EV":
            if "C1A_" in it:
                inserir_evento_na_lista(it, obj, f"C1A{SUFIXO}")
            elif "C18A_" in it:
                inserir_evento_na_lista(it, obj, f"C18A{SUFIXO}")


def parse_frames_placar_ao_vivo(txt: str, now_ts: int):
    touched_c2 = set()

    if txt.startswith(("\x15", "\x14")):
        itens = txt.split("|\x08")
        for item in itens:
            item = item.strip()
            if not item:
                continue

            partes = item[1:].split("\x01", 1)
            if len(partes) != 2:
                continue

            chave = partes[0]
            valor = partes[1]

            if item.startswith("\x14OVInPlay_"):
                global DATA
                DATA = {f"C1A{SUFIXO}": [], f"C18A{SUFIXO}": []}
                inicializar_snapshot_placar(valor)
            elif item.startswith("\x15"):
                aplicar_delta_placar(chave, valor, touched_c2, now_ts)

    return touched_c2


# ============================================================
# PARSER: MERCADOS / ODDS / LINHAS (MG/PA)
# ============================================================
def _remover_chars_controle(s: str) -> str:
    if not s:
        return s
    return s.replace("\x15", "").replace("\x14", "").replace("\x01", "").replace("\x08", "")


def _sanitizar_mercados(obj):
    if isinstance(obj, dict):
        novo = {}
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith("_"):
                continue
            novo[k] = _sanitizar_mercados(v)
        return novo
    if isinstance(obj, list):
        return [_sanitizar_mercados(x) for x in obj]
    return obj


def _mk_key(market_it: str, market_id: str, market_name: str):
    nome = (_clean_str(market_name) or "Mercado Desconhecido").strip()
    return nome


def _normalizar_nome_mercado(nome: str) -> str:
    nome = _clean_str(nome) or "Mercado Desconhecido"
    nome = re.sub(r"\s+", " ", nome.strip())
    return nome


def _mercados_com_chave_por_nome(mercados: dict) -> dict:
    if not isinstance(mercados, dict):
        return {}

    out = {}
    contagem = defaultdict(int)

    for _k, mk in mercados.items():
        if not isinstance(mk, dict):
            continue

        nome = _normalizar_nome_mercado(mk.get("nome_mercado"))
        market_id = _clean_str(mk.get("market_id"))
        market_it = _clean_str(mk.get("market_it"))

        key_nome = nome

        if key_nome in out:
            if market_id:
                key_nome = f"{nome} [{market_id}]"
            elif market_it:
                key_nome = f"{nome} [{market_it}]"
            else:
                contagem[nome] += 1
                key_nome = f"{nome} (#{contagem[nome] + 1})"

        i = 2
        base = key_nome
        while key_nome in out:
            key_nome = f"{base} (#{i})"
            i += 1

        out[key_nome] = mk

    return out


def parse_odds_e_linhas_do_raw(raw: str, now_ts: int):
    resumo = {
        "applied": {"c2": None, "event": None, "fi": None, "market_key": None},
        "stats": {"ev": 0, "mg": 0, "pa": 0, "ignored": 0, "upserts": 0},
        "notes": [],
    }

    if not raw:
        resumo["notes"].append("raw vazio")
        return resumo

    s = _remover_chars_controle(raw)

    if ("PA;" not in s) and ("MG;" not in s) and ("EV;" not in s) and ("MA;" not in s):
        resumo["notes"].append("sem EV/MG/MA/PA (pode ter só U|...)")
        return resumo

    parts = s.split("|")

    c2_evento = None
    nome_evento = None
    fi_corrente = None
    mercado_atual = None
    mercado_key_atual = None

    def garantir_market(store, key_mk, nome_mercado, market_id=None, market_it=None):
        key_mk = _clean_str(key_mk) or "Mercado Desconhecido"

        if key_mk in store["mercados"]:
            existente = store["mercados"][key_mk]
            ex_it = _clean_str(existente.get("market_it"))
            ex_id = _clean_str(existente.get("market_id"))
            if (ex_it and market_it and ex_it != _clean_str(market_it)) or (ex_id and market_id and ex_id != _clean_str(market_id)):
                suf = _clean_str(market_id) or _clean_str(market_it) or "alt"
                key_mk = f"{key_mk} [{suf}]"

        mk = store["mercados"].get(key_mk)
        if not mk:
            mk = {
                "nome_mercado": (nome_mercado or "Mercado Desconhecido").strip(),
                "market_id": _clean_str(market_id),
                "market_it": _clean_str(market_it),
                "suspenso": False,
                "_selecoes_map": {}
            }
            store["mercados"][key_mk] = mk
        else:
            if nome_mercado:
                mk["nome_mercado"] = (nome_mercado or mk.get("nome_mercado") or "Mercado Desconhecido").strip()
            if market_id:
                mk["market_id"] = _clean_str(market_id)
            if market_it:
                mk["market_it"] = _clean_str(market_it)
            if "_selecoes_map" not in mk:
                mk["_selecoes_map"] = {}
        return mk

    def upsert_selecao(mk, selecao, c2_do_evento: str, fi_do_ctx: str):
        sid = _clean_str(selecao.get("selection_id"))
        sit = _clean_str(selecao.get("selection_it"))

        if sid and c2_do_evento:
            SELECTION_ID_TO_C2[sid] = c2_do_evento
        if fi_do_ctx and c2_do_evento:
            EVENTO_POR_FI[fi_do_ctx] = c2_do_evento

        key = sid or sit
        if not key:
            key = f"{selecao.get('nome')}|{selecao.get('linha_ha')}|{selecao.get('linha_hd')}"

        mp = mk["_selecoes_map"]
        old = mp.get(key)

        if not old:
            mp[key] = selecao
            if sid and key != sid:
                mp[sid] = selecao
            resumo["stats"]["upserts"] += 1
            old = selecao
        else:
            if _clean_str(selecao.get("od_frac")):
                old["od_frac"] = _clean_str(selecao["od_frac"])
                old["od_dec"] = odds_para_decimal(old["od_frac"])

            if selecao.get("linha_ha") is not None:
                v = _clean_line(selecao.get("linha_ha"))
                if v is not None:
                    old["linha_ha"] = v
            if selecao.get("linha_hd") is not None:
                v = _clean_line(selecao.get("linha_hd"))
                if v is not None:
                    old["linha_hd"] = v

            # PAREADO COM A BET: suspenso vem APENAS do SU do feed
            old["suspenso"] = bool(selecao.get("suspenso"))

            if _clean_str(selecao.get("nome")):
                old["nome"] = _clean_str(selecao["nome"])
            if _clean_str(selecao.get("n2")):
                old["n2"] = _clean_str(selecao["n2"])
            if _clean_str(selecao.get("ordem")):
                old["ordem"] = _clean_str(selecao.get("ordem"))

            if sid:
                old["selection_id"] = sid
                mp[sid] = old
            if sit:
                old["selection_it"] = sit

            resumo["stats"]["upserts"] += 1

        _touch_selection(old, now_ts)

    for p in parts:
        p = p.strip()
        if len(p) < 3 or p[2] != ";":
            continue

        tag = p[:2]
        corpo = p[3:]
        d = parse_kv_ponto_virgula(corpo)

        if tag == "EV":
            resumo["stats"]["ev"] += 1
            c2_evento = d.get("C2") or c2_evento
            nome_evento = d.get("NA") or nome_evento
            fi = d.get("FI") or d.get("ID") or fi_corrente
            if fi:
                fi_corrente = fi
                resumo["applied"]["fi"] = fi_corrente
                if c2_evento:
                    EVENTO_POR_FI[fi_corrente] = c2_evento
                if nome_evento:
                    NOME_EVENTO_POR_FI[fi_corrente] = nome_evento

            if c2_evento:
                _garantir_store_evento(c2_evento, nome_evento)
                resumo["applied"]["c2"] = c2_evento
                resumo["applied"]["event"] = nome_evento
            continue

        if tag == "MG":
            resumo["stats"]["mg"] += 1
            fi = d.get("FI") or fi_corrente
            if fi:
                fi_corrente = fi
                resumo["applied"]["fi"] = fi_corrente

            if (not c2_evento) and fi_corrente and (fi_corrente in EVENTO_POR_FI):
                c2_evento = EVENTO_POR_FI[fi_corrente]
                nome_evento = NOME_EVENTO_POR_FI.get(fi_corrente)

            if not c2_evento:
                if fi_corrente:
                    c2_try = resolver_c2_por_fi(fi_corrente)
                    if c2_try:
                        c2_evento = c2_try
                        nome_evento = NOME_EVENTO_POR_FI.get(fi_corrente)
            if not c2_evento:
                resumo["stats"]["ignored"] += 1
                continue

            if fi_corrente:
                EVENTO_POR_FI[fi_corrente] = c2_evento
                if nome_evento:
                    NOME_EVENTO_POR_FI[fi_corrente] = nome_evento

            store = _garantir_store_evento(c2_evento, nome_evento)

            nome_mercado = (d.get("NA") or d.get("MN") or d.get("ID") or "Mercado Desconhecido").strip()
            market_id = d.get("ID") or d.get("MA")
            market_it = d.get("IT")

            key_mk = _mk_key(market_it, market_id, nome_mercado)

            mercado_atual = garantir_market(store, key_mk, nome_mercado, market_id=market_id, market_it=market_it)
            mercado_key_atual = key_mk

            if fi_corrente:
                MERCADO_ATUAL_POR_FI[fi_corrente] = key_mk
                MARKET_META_POR_FI[fi_corrente] = {"market_id": market_id, "market_it": market_it, "market_name": nome_mercado}

            _touch_market(mercado_atual, now_ts)

            resumo["applied"]["c2"] = c2_evento
            resumo["applied"]["event"] = nome_evento
            resumo["applied"]["market_key"] = key_mk
            continue

        if tag == "MA":
            fi = d.get("FI") or fi_corrente
            if fi:
                fi_corrente = fi
                resumo["applied"]["fi"] = fi_corrente
            if fi_corrente and fi_corrente in MARKET_META_POR_FI:
                meta = MARKET_META_POR_FI[fi_corrente]
                meta["market_id"] = d.get("ID") or meta.get("market_id")
                meta["market_it"] = d.get("IT") or meta.get("market_it")
            continue

        if tag == "PA":
            resumo["stats"]["pa"] += 1
            fi = d.get("FI") or fi_corrente
            if fi:
                fi_corrente = fi
                resumo["applied"]["fi"] = fi_corrente

            if (not c2_evento) and fi_corrente and (fi_corrente in EVENTO_POR_FI):
                c2_evento = EVENTO_POR_FI[fi_corrente]
                nome_evento = NOME_EVENTO_POR_FI.get(fi_corrente)

            if not c2_evento:
                if fi_corrente:
                    c2_try = resolver_c2_por_fi(fi_corrente)
                    if c2_try:
                        c2_evento = c2_try
                        nome_evento = NOME_EVENTO_POR_FI.get(fi_corrente)

            if not c2_evento:
                resumo["stats"]["ignored"] += 1
                continue

            if fi_corrente:
                EVENTO_POR_FI[fi_corrente] = c2_evento
                if nome_evento:
                    NOME_EVENTO_POR_FI[fi_corrente] = nome_evento

            store = _garantir_store_evento(c2_evento, nome_evento)

            if (mercado_atual is None) and fi_corrente and (fi_corrente in MERCADO_ATUAL_POR_FI):
                key_mk = MERCADO_ATUAL_POR_FI[fi_corrente]
                meta = MARKET_META_POR_FI.get(fi_corrente, {})
                nome_mk = meta.get("market_name") or "Mercado Desconhecido"
                mercado_atual = garantir_market(
                    store,
                    key_mk,
                    nome_mk,
                    market_id=meta.get("market_id"),
                    market_it=meta.get("market_it"),
                )
                mercado_key_atual = key_mk

            if mercado_atual is None:
                resumo["stats"]["ignored"] += 1
                continue

            _touch_market(mercado_atual, now_ts)

            od_frac = _clean_str(d.get("OD"))
            selecao = {
                "nome": _clean_str(d.get("NA")),
                "od_frac": od_frac,
                "od_dec": odds_para_decimal(od_frac) if od_frac else None,
                "linha_ha": _clean_line(d.get("HA")),
                "linha_hd": _clean_line(d.get("HD")),
                "selection_it": _clean_str(d.get("IT")),
                "selection_id": _clean_str(d.get("ID")),
                "suspenso": (str(d.get("SU", "")).strip() == "1"),
                "ordem": _clean_str(d.get("OR")),
                "n2": _clean_str(d.get("N2")),
            }

            if _is_placeholder_selection(selecao):
                resumo["stats"]["ignored"] += 1
                continue

            if not (selecao["nome"] or selecao["od_frac"] or selecao["linha_ha"] or selecao["linha_hd"] or selecao["selection_id"]):
                resumo["stats"]["ignored"] += 1
                continue

            upsert_selecao(mercado_atual, selecao, c2_evento, fi_corrente)
            resumo["applied"]["c2"] = c2_evento
            resumo["applied"]["event"] = nome_evento
            resumo["applied"]["market_key"] = mercado_key_atual
            continue

    return resumo


# ============================================================
# GC opcional: remove coisas antigas (NÃO seta suspenso)
# ============================================================
def gc_stale_for_event(c2: str, now_ts: int):
    if not STALE_ENABLED:
        return

    store = DADOS_MERCADO_POR_EVENTO.get(c2)
    if not store:
        return

    mercados = store.get("mercados", {})
    if not isinstance(mercados, dict):
        return

    removed_mk = []
    for mk_key, mk in list(mercados.items()):
        if not isinstance(mk, dict):
            continue

        mk_last = int(mk.get("_last_seen_ts") or 0)
        if mk_last and (now_ts - mk_last) >= STALE_REMOVE_AFTER_SEC:
            mercados.pop(mk_key, None)
            removed_mk.append(mk_key)

    if removed_mk:
        FRAME_LOG_POR_C2[c2].append({
            "ts": now_ts,
            "summary": {"type": "GC_REMOVE_MARKETS", "removed": removed_mk, "sec": STALE_REMOVE_AFTER_SEC}
        })


# ============================================================
# HELPERS: URL / TARGETS
# ============================================================
def montar_url_partida_por_c2(c2: str) -> str:
    c2 = str(c2).strip()
    return f"{BET_BASE}{TARGET_PREFIXO}{c2}{TARGET_SUFIXO}"


def deve_enviar_target(c2: str, agora_ts: int) -> bool:
    ultimo = ULTIMO_ENVIO_TARGET_TS.get(c2)
    if ultimo is None:
        return True
    return (agora_ts - ultimo) >= TEMPO_ESPERA_TARGETS_SEG


_RE_FI_FROM_EVENT_IT = re.compile(r"^OV(\d{6,})C(?:1A|18A|151A)_")

# ============================================================
# >>> AJUSTE PRINCIPAL: NOMES SINTÉTICOS QUANDO nome == null
# ============================================================
_RE_SPLIT_EVENT = re.compile(r"\s+(?:v|vs\.?|x)\s+", re.IGNORECASE)

def _parse_home_away(event_name: str):
    """
    Tenta extrair "home" e "away" do texto do evento.
    Exemplos: "A v B", "A vs B", "A x B".
    """
    event_name = _clean_str(event_name) or ""
    parts = _RE_SPLIT_EVENT.split(event_name)
    if len(parts) >= 2:
        home = parts[0].strip()
        away = parts[1].strip()
        return (_clean_str(home), _clean_str(away))
    return (None, None)

def _infer_over_under_from_n2(n2: str):
    n2 = _clean_str(n2)
    if not n2:
        return None
    up = n2.upper()
    if up in ("O", "OVER", "OV", "O/U", "OU", "O U"):
        return "Over"
    if up in ("U", "UNDER", "UN", "UND"):
        return "Under"
    if "OVER" in up:
        return "Over"
    if "UNDER" in up:
        return "Under"
    return None

def _is_handicap_market(market_name: str) -> bool:
    mn = (_clean_str(market_name) or "").lower()
    return ("handicap" in mn)

def _is_totals_market(market_name: str) -> bool:
    """
    Markets do tipo linha de gols:
    - Match Goals
    - Goal Line
    - Goals (quando tiver linha)
    """
    mn = (_clean_str(market_name) or "").lower()
    return ("match goals" in mn) or ("goal line" in mn) or (mn.endswith(" goals"))

def _sintetizar_nome_selecao(market_name: str, event_name: str, sel: dict, pos: int, total: int):
    """
    Regras pedidas:
    - Handicap:
      * 3 seleções => Home, Draw, Away (usando nomes reais)
      * 2 seleções => Home, Away
    - Goal lines / Match Goals:
      * "Over X" / "Under X" (se der pra inferir; senão pos 0=Over, pos 1=Under)
    """
    if not isinstance(sel, dict):
        return

    if _clean_str(sel.get("nome")):
        return  # já tem nome

    home, away = _parse_home_away(event_name)
    market_name_clean = _clean_str(market_name) or ""

    # Handicap (2 ou 3 seleções)
    if _is_handicap_market(market_name_clean):
        if total >= 3:
            # pos 0 => home, pos 1 => draw, pos 2 => away
            if pos == 0:
                sel["nome"] = home or "Home"
            elif pos == 1:
                sel["nome"] = "Draw"
            else:
                sel["nome"] = away or "Away"
            return
        if total == 2:
            sel["nome"] = (home or "Home") if pos == 0 else (away or "Away")
            return

    # Totals / Goal lines (Over/Under + linha)
    if _is_totals_market(market_name_clean):
        line = _clean_line(sel.get("linha_ha")) or _clean_line(sel.get("linha_hd"))
        ou = _infer_over_under_from_n2(sel.get("n2"))

        if not ou:
            # fallback por posição: 0=Over, 1=Under
            if total == 2:
                ou = "Over" if pos == 0 else "Under"
            else:
                # se não for 2 seleções, não inventa
                ou = None

        if ou and line:
            sel["nome"] = f"{ou} {line}"
            return
        if ou and not line:
            sel["nome"] = ou
            return

    # fallback final: se for 3 seleções mas não bateu handicap (raro), aplica home/draw/away
    if total >= 3 and home and away:
        if pos == 0:
            sel["nome"] = home
        elif pos == 1:
            sel["nome"] = "Draw"
        else:
            sel["nome"] = away


# ============================================================
# BUILDER DO /live
# ============================================================
def dados_soccer_ao_vivo(incluir_odds: bool = True):
    lista = []
    ev_lst = DATA.get(f"C1A{SUFIXO}", [])
    if not isinstance(ev_lst, list):
        return lista

    agora_ts = ts_agora_utc()

    for ev_it in ev_lst:
        info = DATA.get(ev_it, {})
        if not isinstance(info, dict):
            continue

        try:
            liga = info.get("CT", "")
            if "Esoccer" not in liga:
                continue

            TU = info.get("TU", "")
            TT = int(info.get("TT", 0))
            TS = int(info.get("TS", 0))
            TM = int(info.get("TM", 0))
            MD = info.get("MD", "")

            indexar_ids_evento_se_existirem(info)
            inicio_ts = tu_para_ts_utc(TU)

            if TM == 0 and TT == 0:
                tempo_rel = "00:00"
            else:
                if TT == 1 and inicio_ts is not None:
                    elapsed = max(0, agora_ts - inicio_ts)
                    base = max(0, TM) * 60 + max(0, TS)
                    total = base + elapsed
                    tempo_rel = f"{int(total // 60)}:{int(total % 60):02d}"
                else:
                    tempo_rel = f"{TM}:{TS:02d}"

            if "mins play" in liga:
                try:
                    total_mins = int(liga.split(" - ")[1].split(" ")[0])
                except:
                    total_mins = 90
            else:
                total_mins = 90

            if TM == total_mins / 2 and TS == 0 and TT == 0 and MD == "1":
                periodo = "Intervalo"
            elif TM == total_mins and TS == 0 and TT == 0 and MD == "1":
                periodo = "Fim"
            elif TM == 0 and TS == 0 and TT == 0 and MD == "0":
                periodo = "Vai começar"
            else:
                periodo = "1º Tempo" if MD == "0" else "2º Tempo"

            c2 = str(info.get("C2", "")).strip()
            event_name = info.get("NA", "") or ""

            # aprender FI -> C2
            try:
                fi = None
                m = _RE_FI_FROM_EVENT_IT.search(str(ev_it))
                if m:
                    fi = m.group(1)
                fi = fi or _clean_str(info.get("FI")) or _clean_str(info.get("ID"))
                if fi and c2:
                    EVENTO_POR_FI[fi] = c2
                    na = _clean_str(info.get("NA"))
                    if na:
                        NOME_EVENTO_POR_FI[fi] = na
            except:
                pass

            evento = {
                "event_id": c2,
                "event": event_name,
                "league": liga,
                "time": tempo_rel,
                "score": info.get("SS", ""),
                "period": periodo
            }
            if c2:
                evento["match_url"] = montar_url_partida_por_c2(c2)

            if incluir_odds and c2 and c2 in DADOS_MERCADO_POR_EVENTO:
                if STALE_ENABLED:
                    gc_stale_for_event(c2, agora_ts)

                mercados_internos = DADOS_MERCADO_POR_EVENTO[c2].get("mercados", {})

                # monta selecoes e espelha suspenso (apenas agregação)
                for mk in (mercados_internos or {}).values():
                    if not isinstance(mk, dict):
                        continue
                    mp = mk.get("_selecoes_map", {})
                    if not isinstance(mp, dict):
                        continue

                    vistos = set()
                    lst = []
                    for ssel in mp.values():
                        if not isinstance(ssel, dict):
                            continue
                        sid = _clean_str(ssel.get("selection_id")) or _clean_str(ssel.get("selection_it")) or ""
                        if sid and sid in vistos:
                            continue
                        if sid:
                            vistos.add(sid)

                        ssel["nome"] = _clean_str(ssel.get("nome"))
                        ssel["linha_ha"] = _clean_line(ssel.get("linha_ha"))
                        ssel["linha_hd"] = _clean_line(ssel.get("linha_hd"))
                        ssel["od_frac"] = _clean_str(ssel.get("od_frac"))
                        if ssel.get("od_frac"):
                            ssel["od_dec"] = odds_para_decimal(ssel["od_frac"])

                        ssel["selection_id"] = _clean_str(ssel.get("selection_id"))
                        ssel["selection_it"] = _clean_str(ssel.get("selection_it"))
                        ssel["ordem"] = _clean_str(ssel.get("ordem"))
                        ssel["n2"] = _clean_str(ssel.get("n2"))

                        if _is_placeholder_selection(ssel):
                            continue

                        if not (ssel.get("selection_id") or ssel.get("selection_it") or ssel.get("od_frac") or ssel.get("nome") or ssel.get("linha_ha") or ssel.get("linha_hd")):
                            continue

                        lst.append(ssel)

                    def ordem_int(x):
                        try:
                            return int(str(x.get("ordem", "")).strip())
                        except:
                            return 999999

                    lst.sort(key=lambda x: (ordem_int(x), str(x.get("nome") or "")))

                    # >>> AJUSTE: nomes sintéticos quando nome==null
                    mk_nome = mk.get("nome_mercado")
                    total = len(lst)
                    for i, sel in enumerate(lst):
                        _sintetizar_nome_selecao(mk_nome, event_name, sel, i, total)

                    mk["selecoes"] = lst
                    mk["suspenso"] = (len(lst) > 0 and all(bool(x.get("suspenso")) for x in lst))

                mercados_by_name = _mercados_com_chave_por_nome(mercados_internos)
                evento["mercados"] = mercados_by_name

                # =========================
                # NOVO: extrair "próximo gol" (ex: 12th Goal) com odds oficiais
                # =========================
                total_goals = _parse_total_goals_from_score(evento.get("score"))
                if total_goals is not None:
                    next_n = total_goals + 1  # ex: 11 -> 12
                    evento["next_goal"] = _find_next_goal_market(mercados_by_name, next_n)
                else:
                    evento["next_goal"] = {
                        "expected_market": None,
                        "market_name_found": None,
                        "selecoes": [],
                        "reason": "invalid_score",
                    }

            lista.append(evento)

        except Exception:
            continue

    return _sanitizar_mercados(lista)


# ============================================================
# ROTAS
# ============================================================
@app.route("/data", methods=["POST"])
def handle_data():
    data = request.json or {}
    raw = data.get("data", "")
    now_ts = ts_agora_utc()

    try:
        if DEBUG_PRINT_RAW_LEN:
            print("RAW len:", len(raw))
    except:
        pass

    try:
        if raw:
            ULTIMOS_RAW.append({
                "ts": now_ts,
                "len": len(raw),
                "preview": raw[:DEBUG_RAW_PREVIEW_CHARS]
            })
    except:
        pass

    touched_events = set()

    # aprende FI_inplay <-> FI_delta em todo frame recebido
    try:
        if raw:
            aprender_fi_para_c2_no_mesmo_frame(raw)
    except:
        pass

    try:
        salvar_raw_websocket_leve(raw)

        touched = parse_frames_placar_ao_vivo(raw, now_ts)
        for c2 in (touched or set()):
            touched_events.add(c2)
            FRAME_LOG_POR_C2[c2].append({
                "ts": now_ts,
                "summary": {"type": "U_delta_applied", "c2": c2}
            })
    except Exception as e:
        print("ERRO handle_data (deltas):", repr(e))
        traceback.print_exc()

    try:
        resumo = parse_odds_e_linhas_do_raw(raw, now_ts)
        c2 = (resumo or {}).get("applied", {}).get("c2")
        if c2:
            touched_events.add(c2)
            FRAME_LOG_POR_C2[c2].append({
                "ts": now_ts,
                "summary": resumo,
            })
    except:
        pass

    # ring buffer para dump de gol
    try:
        if raw and touched_events:
            for c2x in touched_events:
                RAW_RING_BY_C2[c2x].append(raw)

                pend = PENDING_GOAL.get(c2x)
                if pend:
                    pend["after_left"] = int(pend.get("after_left") or 0) - 1
                    if pend["after_left"] <= 0:
                        dump_goal_file(c2x)
                        PENDING_GOAL.pop(c2x, None)
    except:
        pass

    return "1"


@app.route("/debug_raw", methods=["GET"])
def debug_raw():
    return jsonify(list(ULTIMOS_RAW)), 200


@app.route("/live", methods=["GET"])
def live_event():
    incluir_odds = request.args.get("odds", "1") != "0"
    lista = dados_soccer_ao_vivo(incluir_odds=incluir_odds)
    return jsonify(lista), 200


@app.route("/markets", methods=["GET"])
def markets():
    c2 = (request.args.get("c2") or "").strip()
    if c2:
        st = DADOS_MERCADO_POR_EVENTO.get(c2, {})
        if isinstance(st, dict) and "mercados" in st:
            st = dict(st)
            st["mercados"] = _mercados_com_chave_por_nome(st.get("mercados", {}))
        return jsonify(_sanitizar_mercados(st)), 200
    return jsonify(_sanitizar_mercados(DADOS_MERCADO_POR_EVENTO)), 200


@app.route("/explicar_frame", methods=["GET"])
def explicar_frame():
    c2 = (request.args.get("c2") or "").strip()
    if not c2:
        return jsonify({"erro": "Informe o parâmetro ?c2=ID_DO_EVENTO"}), 400

    frames = list(FRAME_LOG_POR_C2.get(c2, []))
    snapshot = DADOS_MERCADO_POR_EVENTO.get(c2, {})

    return jsonify(_sanitizar_mercados({
        "evento_c2": c2,
        "frames_capturados": len(frames),
        "frames": frames,
        "snapshot_atual": snapshot,
        "politica_suspenso": {
            "pareado_com_bet": True,
            "regra": "suspenso só muda quando SU vem no feed (PA.SU ou delta U com SU).",
            "no_goal_suspend": "NÃO suspende mercados no gol; faz PURGE de goal markets ao detectar gol.",
            "gc": {
                "enabled": STALE_ENABLED,
                "obs": "Se ligado, GC só REMOVE mercados antigos; não seta suspenso."
            }
        }
    })), 200


@app.route("/targets", methods=["GET"])
def targets():
    limit = int(request.args.get("limit", "15"))

    cooldown = request.args.get("cooldown")
    if cooldown is not None:
        try:
            cd = int(cooldown)
            if cd >= 0:
                global TEMPO_ESPERA_TARGETS_SEG
                TEMPO_ESPERA_TARGETS_SEG = cd
        except:
            pass

    agora_ts = ts_agora_utc()
    live = dados_soccer_ao_vivo(incluir_odds=False)

    urls = []
    for ev in live:
        c2 = str(ev.get("event_id", "")).strip()
        if not c2:
            continue

        if not deve_enviar_target(c2, agora_ts):
            continue

        urls.append(montar_url_partida_por_c2(c2))
        ULTIMO_ENVIO_TARGET_TS[c2] = agora_ts

        if len(urls) >= limit:
            break

    return jsonify({"urls": urls}), 200


@app.route("/active_ids", methods=["GET"])
def active_ids():
    live = dados_soccer_ao_vivo(incluir_odds=False)
    ids = []
    for ev in live:
        c2 = str(ev.get("event_id", "")).strip()
        if c2:
            ids.append(c2)
    return jsonify({"ids": ids}), 200


@app.route("/active_map", methods=["GET"])
def active_map():
    live = dados_soccer_ao_vivo(incluir_odds=False)
    mp = {}
    for ev in live:
        c2 = str(ev.get("event_id", "")).strip()
        if not c2:
            continue
        mp[c2] = {
            "period": ev.get("period"),
            "time": ev.get("time"),
            "league": ev.get("league"),
            "event": ev.get("event")
        }
    return jsonify(mp), 200


@app.route("/")
def root():
    return render_template("dashboard.html")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@app.route("/process_dump", methods=["GET"])
def process_dump():
    path = (request.args.get("path") or "").strip()
    if not path:
        return jsonify({"erro": "use ?path=CAMINHO_DO_ARQUIVO"}), 400

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            txt = f.read()

        blocks = []
        cur = []
        for line in txt.splitlines():
            if line.startswith("--- ts="):
                if cur:
                    blocks.append("\n".join(cur))
                    cur = []
                continue
            if line.startswith("== GOAL DETECTED ==") or line.startswith("===== "):
                continue
            cur.append(line)
        if cur:
            blocks.append("\n".join(cur))

        processed = 0
        for raw in blocks:
            raw = raw.strip()
            if not raw:
                continue
            now_ts = ts_agora_utc()

            aprender_fi_para_c2_no_mesmo_frame(raw)
            parse_frames_placar_ao_vivo(raw, now_ts)
            parse_odds_e_linhas_do_raw(raw, now_ts)

            processed += 1

        return jsonify({"ok": True, "blocks": processed}), 200
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8485, threaded=True, debug=False)
