const prevOdds = new Map(); // key -> number
let timer = null;

function nowStr() {
  return new Date().toLocaleTimeString();
}

function formatOdd(x) {
  if (x === null || x === undefined || x === "") return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return n.toFixed(2);
}

function oddClass(key, value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  const prev = prevOdds.get(key);
  prevOdds.set(key, n);
  if (prev === undefined) return "";
  if (n > prev) return "up";
  if (n < prev) return "down";
  return "";
}

// Seu backend já retorna:
// event = { id, event, league, time, score, period, odds? }
// odds = { "Market Name": [ {selection, odds_decimal, odds_fractional, selection_id}, ... ] }
function renderMatch(m) {
  const id = m.id ?? "";
  const title = m.event ?? "";
  const league = m.league ?? "";
  const time = m.time ?? "";
  const score = m.score ?? "";
  const period = m.period ?? "";

  const oddsObj = m.odds && typeof m.odds === "object" ? m.odds : null;

  let oddsHtml = `<div class="muted small">Odds: desativadas</div>`;
  if (oddsObj) {
    const markets = Object.keys(oddsObj);
    if (markets.length === 0) {
      oddsHtml = `<div class="muted small">Sem odds disponíveis</div>`;
    } else {
      // mostra no máximo 3 mercados pra não lotar (você pode aumentar)
      const topMarkets = markets.slice(0, 3);
      oddsHtml = topMarkets.map(mktName => {
        const sels = Array.isArray(oddsObj[mktName]) ? oddsObj[mktName] : [];
        const rows = sels.slice(0, 8).map(s => {
          const nm = s.selection ?? ("Selection " + (s.selection_id ?? ""));
          const od = s.odds_decimal ?? s.odds_fractional ?? "";
          const key = `${id}::${mktName}::${s.selection_id ?? nm}`;
          const cls = oddClass(key, od);
          return `
            <div class="selRow ${cls}">
              <div class="selName">${nm}</div>
              <div class="selOdd">${formatOdd(od)}</div>
            </div>
          `;
        }).join("");

        return `
          <div class="market">
            <div class="marketTitle">${mktName}</div>
            <div class="marketGrid">${rows || `<div class="muted small">Sem seleções</div>`}</div>
          </div>
        `;
      }).join("");
    }
  }

  return `
    <section class="match" data-id="${id}">
      <div class="matchHead">
        <div class="teams">
          <div>
            <div class="team">${title || "<span class='muted'>(sem nome)</span>"}</div>
            <div class="small muted">${league}</div>
          </div>
          <div class="scoreBox">
            <div class="score">${score || "—"}</div>
            <div class="small muted">${time} • ${period}</div>
          </div>
        </div>
        <div class="meta">
          <div class="muted">id: ${id}</div>
          <div class="muted">live</div>
        </div>
      </div>

      <div class="odds">
        ${oddsHtml}
      </div>
    </section>
  `;
}

async function fetchLive() {
  const sport = document.getElementById("sport").value;
  const odds = document.getElementById("odds").value;
  const filter = document.getElementById("filter").value.trim().toLowerCase();

  const t0 = performance.now();
  document.getElementById("status").textContent = "atualizando…";

  const res = await fetch(`/live?sport=${encodeURIComponent(sport)}&odds=${encodeURIComponent(odds)}`, { cache: "no-store" });
  const t1 = performance.now();

  document.getElementById("lat").textContent = Math.round(t1 - t0) + "ms";
  document.getElementById("last").textContent = nowStr();

  if (!res.ok) throw new Error("HTTP " + res.status);
  let list = await res.json();
  if (!Array.isArray(list)) list = [];

  if (filter) {
    list = list.filter(m => JSON.stringify(m).toLowerCase().includes(filter));
  }

  document.getElementById("count").textContent = String(list.length);
  document.getElementById("grid").innerHTML = list.map(renderMatch).join("");
  document.getElementById("status").textContent = "ok";
}

function start() {
  const interval = Math.max(250, Number(document.getElementById("interval").value) || 1000);

  if (timer) clearInterval(timer);

  fetchLive().catch(err => {
    console.error(err);
    document.getElementById("status").textContent = "erro";
  });

  timer = setInterval(() => {
    fetchLive().catch(err => {
      console.error(err);
      document.getElementById("status").textContent = "erro";
    });
  }, interval);
}

["sport","odds","interval"].forEach(id => {
  document.getElementById(id).addEventListener("change", start);
});
document.getElementById("filter").addEventListener("input", () => {
  clearTimeout(window.__filterT);
  window.__filterT = setTimeout(start, 250);
});

start();
