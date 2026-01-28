# betws/state.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from collections import defaultdict

from .core import Record, Segment, split_raw_into_records, parse_init_payload_to_segments, parse_update_payload
from .odds import odds_to_decimal

@dataclass
class Selection:
    selection_id: str
    name: str = ""
    odds_fractional: str = ""
    odds_decimal: Optional[float] = None
    order: int = 999
    ha: Optional[str] = None   # handicap/line raw
    hd: Optional[str] = None   # split
    n2: Optional[str] = None   # 1/X/2, Over/Under etc
    su: Optional[str] = None   # suspended
    raw_it: Optional[str] = None

@dataclass
class Market:
    market_id: str
    name: str = ""
    order: int = 999
    py: Optional[str] = None
    sy: Optional[str] = None
    cn: Optional[str] = None
    line_text: Optional[str] = None  # ex: "4.5" ou "4.5,5.0"
    selections: Dict[str, Selection] = field(default_factory=dict)

@dataclass
class Event:
    oi: str  # internal
    c2: Optional[str] = None
    name: str = ""
    league: str = ""
    score: str = ""
    md: Optional[str] = None
    tm: Optional[str] = None
    ts: Optional[str] = None
    tu: Optional[str] = None
    tt: Optional[str] = None
    markets: Dict[str, Market] = field(default_factory=dict)

class BetWsState:
    """
    Estado global.
    - mantém DATA-like (items brutos) se você quiser
    - mas principalmente mantém Event/Market/Selection padronizado para consumo.
    """

    def __init__(self, suffix: str = "_1_3"):
        self.suffix = suffix

        # mapeamentos úteis (parecido com seus dicts)
        self.event_c2_to_oi: Dict[str, str] = {}

        # contexto de MA->PA por FI
        self.last_market_name_by_fi: Dict[str, str] = {}
        self.last_market_it_by_fi: Dict[str, str] = {}

        self.selection_it_to_market_name: Dict[str, str] = {}
        self.selection_it_to_market_it: Dict[str, str] = {}

        self.selection_name_by_id: Dict[str, str] = {}

        # estado alto nível
        self.events_by_oi: Dict[str, Event] = {}

        # índice rápido selection_id -> (oi, market_id)
        self.selection_to_owner: Dict[str, Tuple[str, str]] = {}

    def reset(self):
        self.__init__(suffix=self.suffix)

    # ---------- helpers de contexto ----------
    def _index_event_ids_if_present(self, fields: Dict[str, str]):
        c2 = fields.get("C2")
        oi = fields.get("OI")
        if c2 and oi:
            self.event_c2_to_oi[str(c2)] = str(oi)

    def _remember_market_context(self, seg: Segment):
        # aprende nome de selection por ID
        sid = seg.fields.get("ID")
        na = (seg.fields.get("NA") or "").strip()
        if sid and na:
            self.selection_name_by_id[str(sid)] = na

        fi = seg.fields.get("FI")
        if not fi:
            return
        fi = str(fi)

        if seg.tag == "MA":
            mname = (seg.fields.get("NA") or "").strip()
            if mname:
                self.last_market_name_by_fi[fi] = mname
            it = seg.fields.get("IT")
            if it:
                self.last_market_it_by_fi[fi] = it

        elif seg.tag == "PA":
            it = seg.fields.get("IT")
            if not it:
                return
            mname = self.last_market_name_by_fi.get(fi)
            mit = self.last_market_it_by_fi.get(fi)
            if mname:
                self.selection_it_to_market_name[it] = mname
            if mit:
                self.selection_it_to_market_it[it] = mit

    # ---------- apply snapshot/init ----------
    def apply_init(self, payload: str):
        segs = parse_init_payload_to_segments(payload)

        current_event_oi: Optional[str] = None
        current_market_id: Optional[str] = None

        for seg in segs:
            self._index_event_ids_if_present(seg.fields)
            self._remember_market_context(seg)

            if seg.tag == "EV":
                oi = seg.fields.get("OI") or seg.fields.get("FI")  # fallback
                if not oi:
                    continue
                oi = str(oi)
                current_event_oi = oi

                ev = self.events_by_oi.get(oi) or Event(oi=oi)
                ev.c2 = seg.fields.get("C2") or ev.c2
                ev.name = seg.fields.get("NA") or ev.name
                ev.league = seg.fields.get("CT") or ev.league
                ev.score = seg.fields.get("SS") or ev.score
                ev.md = seg.fields.get("MD") or ev.md
                ev.tm = seg.fields.get("TM") or ev.tm
                ev.ts = seg.fields.get("TS") or ev.ts
                ev.tu = seg.fields.get("TU") or ev.tu
                ev.tt = seg.fields.get("TT") or ev.tt

                self.events_by_oi[oi] = ev
                if ev.c2:
                    self.event_c2_to_oi[str(ev.c2)] = oi

            elif seg.tag == "MG":
                # cabeçalho de mercado
                if not current_event_oi:
                    continue
                market_id = seg.fields.get("ID")
                if not market_id:
                    continue
                market_id = str(market_id)
                current_market_id = market_id

                ev = self.events_by_oi[current_event_oi]
                mk = ev.markets.get(market_id) or Market(market_id=market_id)
                mk.name = seg.fields.get("NA") or mk.name
                mk.order = int(seg.fields.get("OR", mk.order)) if str(seg.fields.get("OR", "")).isdigit() else mk.order
                mk.sy = seg.fields.get("SY") or mk.sy
                ev.markets[market_id] = mk

            elif seg.tag == "MA":
                # config do mercado (CN, PY, etc). Também pode conter line text em alguns tipos.
                if not current_event_oi or not current_market_id:
                    continue
                ev = self.events_by_oi[current_event_oi]
                mk = ev.markets.get(current_market_id)
                if not mk:
                    continue
                mk.py = seg.fields.get("PY") or mk.py
                mk.cn = seg.fields.get("CN") or mk.cn
                mk.sy = seg.fields.get("SY") or mk.sy
                # alguns markets carregam "NA=" em MA (ex: Correct Score label)
                if seg.fields.get("NA"):
                    # cuidado: aqui é nome do mercado também; não sobrescreve se vazio
                    pass

            elif seg.tag == "PA":
                # selection com odds / nomes / HA/HD
                if not current_event_oi or not current_market_id:
                    continue
                ev = self.events_by_oi[current_event_oi]
                mk = ev.markets.get(current_market_id)
                if not mk:
                    continue

                sel_id = seg.fields.get("ID")
                if not sel_id:
                    continue
                sel_id = str(sel_id)

                name = (seg.fields.get("NA") or "").strip()
                if not name:
                    name = self.selection_name_by_id.get(sel_id, "")

                od = (seg.fields.get("OD") or "").strip()
                sel = mk.selections.get(sel_id) or Selection(selection_id=sel_id)
                sel.name = name or sel.name
                sel.odds_fractional = od or sel.odds_fractional
                sel.odds_decimal = odds_to_decimal(sel.odds_fractional)
                sel.order = int(seg.fields.get("OR", sel.order)) if str(seg.fields.get("OR", "")).isdigit() else sel.order
                sel.ha = seg.fields.get("HA") or sel.ha
                sel.hd = seg.fields.get("HD") or sel.hd
                sel.n2 = seg.fields.get("N2") or sel.n2
                sel.su = seg.fields.get("SU") or sel.su
                sel.raw_it = seg.fields.get("IT") or sel.raw_it

                mk.selections[sel_id] = sel
                self.selection_to_owner[sel_id] = (current_event_oi, current_market_id)

                # linha textual (ex: Match Goals tem um PA “linha” sem OD)
                # seu dump mostra um PA com NA=4.5 e sem OD, que representa a linha
                if sel.name and (sel.odds_fractional in ("", "0/0")):
                    # se isso parece uma linha (número ou "a,b"), salva
                    txt = sel.name.strip()
                    if any(ch.isdigit() for ch in txt):
                        mk.line_text = mk.line_text or txt

    # ---------- apply update ----------
    def apply_update(self, action_key: str, payload: str):
        """
        Update simples: geralmente vem OD=...; em L... / OV... targets.
        Seu código atual salva isso em DATA. Aqui a gente atualiza no estado alto nível,
        se conseguir descobrir selection_id no action_key.
        """
        d = parse_update_payload("U", payload)
        if not d:
            return

        # tenta extrair selection id do action_key padrão: L<fixture>-<sel>_<suffix> ...
        sel_id = self._extract_selection_id(action_key)
        if sel_id and sel_id in self.selection_to_owner:
            oi, mid = self.selection_to_owner[sel_id]
            ev = self.events_by_oi.get(oi)
            if not ev:
                return
            mk = ev.markets.get(mid)
            if not mk:
                return
            sel = mk.selections.get(sel_id)
            if not sel:
                return

            if "OD" in d:
                sel.odds_fractional = d["OD"]
                sel.odds_decimal = odds_to_decimal(sel.odds_fractional)
            if "SU" in d:
                sel.su = d["SU"]

    def _extract_selection_id(self, action_key: str) -> Optional[str]:
        """
        Ex: action_key = "L188665548-469947664_1_3"  -> selection_id=469947664
        Ex: action_key = "OV188665548-469947664_1_3" -> selection_id=469947664
        """
        k = action_key.strip()

        # remove prefixos conhecidos
        if k.startswith("OV"):
            k2 = k[2:]
        else:
            k2 = k

        # agora k2 deve começar com "L" ou direto numero
        if k2.startswith("L"):
            k2 = k2[1:]

        # formato: "<fixture>-<sel>_<suffix>"
        if "-" not in k2:
            return None
        left, right = k2.split("-", 1)
        # right: "469947664_1_3"
        if "_" in right:
            sel = right.split("_", 1)[0]
        else:
            sel = right
        sel = sel.strip()
        return sel if sel.isdigit() else None

    # ---------- ingest raw ----------
    def ingest_raw(self, raw: str):
        """
        Entrada única: raw WS.
        - detecta init (OVInPlay_) e reseta estado
        - aplica updates (U) continuamente
        """
        recs = split_raw_into_records(raw)
        for r in recs:
            # init/snapshot: seu raw mostra item começando com "\x14OVInPlay_"
            # aqui a gente detecta pelo action_key
            if r.action_key.startswith("OVInPlay_") or r.action_key.startswith("OVInPlay"):
                # payload vem como snapshot com vários '|'
                self.reset()
                self.apply_init(r.payload)
                continue

            if r.action == "U":
                self.apply_update(r.action_key, r.payload)
            elif r.action == "I":
                # alguns streams mandam inserts fora do init;
                # dá pra tratar como init parcial: payload = "EV;..." etc
                # aqui, para simplificar, tenta parsear como segmentos isolados
                self.apply_init("X|" + r.payload)  # hack: cria split consistente
