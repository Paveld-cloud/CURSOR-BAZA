const tg = window.Telegram?.WebApp;

if (tg) {
  tg.expand();
  tg.setHeaderColor?.('#121212');
  tg.setBackgroundColor?.('#0b1220');
  tg.ready();
}

const qInput   = document.getElementById("q");
const btnFind  = document.getElementById("btnFind");
const btnClear = document.getElementById("btnClear");

const statusEl = document.getElementById("status");
const foundEl  = document.getElementById("found");
const listEl   = document.getElementById("list");

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
  statusEl.textContent = text || "";
  statusEl.style.color =
    kind === "error" ? "rgba(255,140,140,0.95)" :
    kind === "ok" ? "rgba(140,255,190,0.90)" :
    "rgba(255,255,255,0.62)";
}

function toNum(x) {
  const s = String(x ?? "").trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

async function safeJson(res) {
  try { return await res.json(); } catch { return null; }
}

function getField(row, keys, def = "—") {
  for (const k of keys) {
    if (row && row[k] !== undefined && row[k] !== null && String(row[k]).trim() !== "") {
      return String(row[k]).trim();
    }
  }
  return def;
}

function renderItemCard(row) {
  const code = getField(row, ["код", "code", "КОД"], "");
  const name = getField(row, ["наименование", "name"], "Без наименования");
  const type = getField(row, ["тип", "type"], "—");
  const part = getField(row, ["парт номер", "part_number", "part"], "—");
  const oem  = getField(row, ["oem парт номер", "OEM парт номер", "oem"], "—");
  const qty  = getField(row, ["количество", "остаток", "qty"], "—");
  const price= getField(row, ["цена", "price"], "—");
  const cur  = getField(row, ["валюта", "currency"], "");
  const mfg  = getField(row, ["изготовитель", "manufacturer"], "—");

  // backend у тебя обычно отдаёт image_url или image
  const imageUrl = getField(row, ["image_url", "image", "photo"], "");

  const photoHtml = imageUrl && imageUrl !== "—"
    ? `<div class="itemPhoto"><img class="photo" src="${esc(imageUrl)}" alt="photo"></div>`
    : `<div class="itemPhoto"><div class="noPhoto">Нет фото</div></div>`;

  // кнопка "Описание" УБРАНА — всё уже тут
  return `
    <div class="item" data-code="${esc(code)}">
      ${photoHtml}
      <div class="itemBody">
        <div class="title">${esc(name)}</div>

        <div class="meta">
          <div><b>Тип:</b> ${esc(type)}</div>
          <div><b>Part №:</b> ${esc(part)}</div>
          <div><b>OEM:</b> ${esc(oem)}</div>
          <div><b>Количество:</b> ${esc(qty)}</div>
          <div><b>Цена:</b> ${esc(price)} ${esc(cur)}</div>
          <div><b>Изготовитель:</b> ${esc(mfg)}</div>
        </div>

        <div class="btnRow">
          <button class="btn issueBtn" data-code="${esc(code)}">📦 Взять деталь</button>
          <button class="btn btn--ghost copyBtn" data-code="${esc(code)}">📋 Копировать код</button>
        </div>
      </div>
    </div>
  `;
}

function renderList(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    foundEl.textContent = "0";
    listEl.innerHTML = "";
    setStatus("Ничего не найдено", "muted");
    return;
  }

  foundEl.textContent = String(rows.length);
  listEl.innerHTML = rows.map(renderItemCard).join("");

  // handlers
  listEl.querySelectorAll(".copyBtn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      const code = e.currentTarget.getAttribute("data-code") || "";
      if (!code) return;
      try {
        await navigator.clipboard.writeText(code);
        setStatus("Код скопирован", "ok");
      } catch {
        setStatus("Не удалось скопировать код", "error");
      }
    });
  });

  listEl.querySelectorAll(".issueBtn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      const code = e.currentTarget.getAttribute("data-code") || "";
      if (!code) return;

      const qtyStr = prompt("Сколько списать? (пример: 1 или 2.5)");
      if (!qtyStr) return;

      const qtyNum = toNum(qtyStr);
      if (qtyNum === null || qtyNum <= 0) {
        alert("Введите корректное количество.");
        return;
      }

      const comment = (prompt("Комментарий (пример: OP-1100 авария, замена датчика)") || "").trim();

      setStatus("Отправляю списание…", "muted");

      const payload = {
        user_id: userId(),
        name: userName(),
        code: code,
        qty: qtyNum,
        comment: comment
      };

      const res = await fetch("/app/api/issue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
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

  setStatus("", "ok");
}

async function doSearch() {
  const q = (qInput?.value || "").trim();
  if (!q) {
    setStatus("Введите код / part № / OEM / наименование", "muted");
    return;
  }

  setStatus("Ищу…", "muted");
  foundEl.textContent = "…";
  listEl.innerHTML = "";

  const res = await fetch(
    `/app/api/search?q=${encodeURIComponent(q)}&user_id=${encodeURIComponent(userId())}`,
    { cache: "no-store" }
  );
  const data = await safeJson(res);

  if (!res.ok || !data || !data.ok) {
    setStatus(data?.error || `Ошибка поиска (${res.status})`, "error");
    foundEl.textContent = "0";
    return;
  }

  // backend может отдавать rows/items/results — поддержим все варианты
  const rows = data.rows || data.items || data.results || [];
  renderList(rows);
}

function clearAll() {
  if (qInput) qInput.value = "";
  foundEl.textContent = "0";
  listEl.innerHTML = "";
  setStatus("", "muted");
  qInput?.focus?.();
}

btnFind?.addEventListener("click", doSearch);
btnClear?.addEventListener("click", clearAll);

qInput?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

