const tg = window.Telegram?.WebApp;
if (tg) {
  tg.expand();
  tg.ready();
}

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clearBtn = document.getElementById("clear");
const st = document.getElementById("st");
const list = document.getElementById("list");
const meta = document.getElementById("meta");
const envPill = document.getElementById("envPill");

function userId() { return tg?.initDataUnsafe?.user?.id || 0; }
function userName() {
  const u = tg?.initDataUnsafe?.user;
  if (!u) return "";
  const fn = (u.first_name || "").trim();
  const ln = (u.last_name || "").trim();
  return (fn + " " + ln).trim() || (u.username ? "@" + u.username : "");
}

function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function setStatus(text, kind = "muted") {
  st.textContent = text || "";
  st.style.color =
    kind === "error" ? "rgba(255,140,140,0.95)" :
    kind === "ok" ? "rgba(140,255,190,0.90)" :
    "rgba(255,255,255,0.62)";
}

function setMeta(text) {
  if (meta) meta.textContent = text || "—";
}

async function safeJson(res) {
  try { return await res.json(); } catch { return null; }
}

function toNum(x) {
  const s = String(x ?? "").trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

// --- UI helpers ---
function renderEmpty(text) {
  list.innerHTML = `
    <div class="item">
      <div class="item__sub">${esc(text || "Ничего не найдено")}</div>
    </div>
  `;
}

function renderItems(items) {
  list.innerHTML = "";

  for (const it of items) {
    const codeRaw = (it["код"] || it.code || "").toString();
    const code = codeRaw.trim().toLowerCase();

    const name = (it["наименование"] || it.name || "").toString();
    const typ = (it["тип"] || it.type || "").toString();
    const oem = (it["oem"] || it["oem парт номер"] || it.oem || "").toString();
    const part = (it["парт номер"] || it["парт №"] || it.part || "").toString();
    const qty = (it["количество"] ?? it.qty ?? "").toString();
    const price = (it["цена"] ?? it.price ?? "").toString();
    const cur = (it["валюта"] ?? it.currency ?? "").toString();

    const priceText = (price || cur) ? `${price} ${cur}`.trim() : "—";

    const html = `
      <div class="item" data-code="${esc(code)}">
        <div class="item__top">
          <div class="badge">Код: ${esc(codeRaw || "—")}</div>
          <div class="badge">Остаток: ${esc(qty || "—")}</div>
        </div>

        <div class="item__title">${esc(name || "Без наименования")}</div>

        <div class="item__sub">
          <div><b>Тип:</b> ${esc(typ || "—")}</div>
          ${part ? `<div><b>Part №:</b> ${esc(part)}</div>` : ``}
          ${oem ? `<div><b>OEM:</b> ${esc(oem)}</div>` : ``}
          <div><b>Цена:</b> ${esc(priceText)}</div>
        </div>

        <div class="item__actions">
          <button class="item__btn item__btn--primary" data-issue="${esc(code)}">📦 Взять деталь</button>
          <button class="item__btn" data-info="${esc(code)}">ℹ️ Описание</button>
        </div>
      </div>
    `;

    list.insertAdjacentHTML("beforeend", html);
  }

  // handlers
  list.querySelectorAll("[data-info]").forEach((b) => {
    b.addEventListener("click", () => {
      const code = b.getAttribute("data-info") || "";
      // у тебя маршрут в aiohttp: /app/item
      window.location.href = `/app/item?code=${encodeURIComponent(code)}`;
    });
  });

  list.querySelectorAll("[data-issue]").forEach((b) => {
    b.addEventListener("click", async () => {
      const code = b.getAttribute("data-issue") || "";

      const qtyStr = prompt("Сколько списать? (пример: 1 или 2.5)");
      if (!qtyStr) return;

      const qtyNum = toNum(qtyStr);
      if (qtyNum === null || qtyNum <= 0) {
        alert("Введите корректное количество.");
        return;
      }

      const comment = (prompt("Комментарий (пример: OP-1100 авария, замена датчика)") || "").trim();

      const payload = {
        user_id: userId(),
        name: userName(),
        code: code,
        qty: qtyNum,
        comment: comment,
      };

      setStatus("Отправляю списание...", "muted");

      const res = await fetch("/api/issue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const out = await safeJson(res);

      if (!res.ok || !out || !out.ok) {
        const err = out?.error || `Ошибка списания (${res.status})`;
        setStatus(err, "error");
        alert(err);
        return;
      }

      setStatus("✅ Списание записано в История", "ok");
      alert("✅ Списание записано в История");
    });
  });
}

async function doSearch() {
  const text = (q.value || "").trim();
  if (!text) { setStatus("Введите запрос", "error"); return; }

  setStatus("Ищу...", "muted");
  setMeta("—");
  list.innerHTML = "";
  if (envPill) envPill.textContent = "Поиск…";

  // API у тебя сейчас на /api/search
  const url = `/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;

  const res = await fetch(url);
  const data = await safeJson(res);

  if (envPill) envPill.textContent = "Online";

  if (!res.ok || !data || !data.ok) {
    const err = data?.error || `Ошибка поиска (${res.status})`;
    setStatus(err, "error");
    renderEmpty("Ошибка поиска. Проверь соединение/сервер.");
    return;
  }

  const items = data.items || [];
  setStatus(items.length ? "" : "Ничего не найдено", items.length ? "ok" : "muted");
  setMeta(`Найдено: ${items.length}`);

  if (!items.length) {
    renderEmpty("Ничего не найдено");
    return;
  }

  renderItems(items);
}

// --- wiring ---
btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });

clearBtn?.addEventListener("click", () => {
  q.value = "";
  setStatus("");
  setMeta("—");
  list.innerHTML = "";
  q.focus();
});

// подсветка статуса при старте
if (envPill) envPill.textContent = "Online";
setStatus("");
setMeta("—");

