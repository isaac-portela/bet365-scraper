# betws/core.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

CTRL_STARTS = ("\x14", "\x15")   # seus exemplos começam com esses
SEP_RECORD = "|\x08"            # separador típico nos dumps

@dataclass
class Record:
    """
    Uma unidade de ação: action_key (target) + action (I/U/D/F-ish) + payload.
    """
    action_key: str
    action: str
    payload: str
    raw: str

@dataclass
class Segment:
    """
    Um bloco tipo EV;... ou MG;... ou PA;... dentro de um payload F/Init.
    """
    tag: str
    fields: Dict[str, str]
    raw: str

def parse_kv_semicolon(body: str) -> Dict[str, str]:
    """
    Parser do estilo 'A=1;B=2;C=;'
    Observação: seu to_dit atual corta o último char; aqui não depende disso.
    """
    out: Dict[str, str] = {}
    if not body:
        return out

    # garante que termina com ';' pra facilitar split
    if not body.endswith(";"):
        body = body + ";"

    parts = body.split(";")
    for p in parts:
        if not p:
            continue
        if "=" in p:
            k, v = p.split("=", 1)
            out[k] = v
    return out

def split_raw_into_records(raw: str) -> List[Record]:
    """
    Quebra o raw do WS em Records.
    Seu formato é: <CTRL><action_key>\x01<action_val>... repetido
    """
    if not raw:
        return []

    # normaliza: alguns frames vêm com \x15 ou \x14 no começo
    if not raw.startswith(CTRL_STARTS):
        # mesmo assim pode conter registros; tenta no fallback
        raw2 = raw
    else:
        raw2 = raw

    items = [x.strip() for x in raw2.split(SEP_RECORD) if x.strip()]
    recs: List[Record] = []

    for item in items:
        if len(item) < 2:
            continue
        # item começa com \x14 ou \x15
        lead = item[0]
        rest = item[1:]
        # split em action_key e action_val
        chunks = rest.split("\x01", 1)
        if len(chunks) != 2:
            continue
        action_key, action_val = chunks[0], chunks[1]

        # action_val geralmente é "U|...." ou "I|...."
        if "|" not in action_val:
            continue
        action, payload = action_val.split("|", 1)
        action = action.strip()

        recs.append(Record(
            action_key=action_key,
            action=action,
            payload=payload,
            raw=item
        ))

    return recs

def parse_init_payload_to_segments(payload: str) -> List[Segment]:
    """
    payload do init geralmente é algo como:
      <algo>|EV;...|TG;...|TE;...|MG;...|MA;...|PA;...|...
    No seu código, você faz: txt.split('|')[1:]
    Aqui aceitamos payload que já vem como tudo após "F|".
    """
    segs: List[Segment] = []

    # payload pode vir com um prefixo antes do primeiro '|'
    parts = payload.split("|")
    for part in parts:
        part = part.strip()
        if len(part) < 3:
            continue

        # exemplo: "EV;AM=...;C2=...;"
        if ";" not in part:
            continue

        tag = part[:2]  # EV, MG, MA, PA, TE, SC, SL...
        # parte pode ser "EV;...." ou "MA;...."
        body = part[3:] if len(part) >= 3 else ""
        fields = parse_kv_semicolon(body)
        segs.append(Segment(tag=tag, fields=fields, raw=part))

    return segs

def parse_update_payload(action: str, payload: str) -> Dict[str, str]:
    """
    Para action=U, seu payload é um body "OD=..;SS=..;"
    Para action=I, payload começa com "EV;..." "PA;..."
    Aqui apenas retorna dict básico para U.
    """
    if action == "U":
        return parse_kv_semicolon(payload)
    return {}
