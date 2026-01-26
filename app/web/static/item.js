const tg = window.Telegram?.WebApp;
if (tg) {
  tg.expand();
  tg.ready();
}

const codeLine = document.getElementById("codeLine");
const envPill = document.getElementById("envPill");
const statusEl = document.getElementById("status");
const box = document.getElementById("box");
const img = document.getElementById("img");
const photoWrap = document.getElementById("photoWrap");
const backBtn = document.getElementById("back");
const issueBtn = document.getElementById("issue");

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

function getCodeFromUrl() {
  const u = new URL(window.location.href);
  return (u.searchParams.get("code") || "").trim().toLowerCase();
}

let CURRENT = { code: "", item: null };

function renderItem(item) {
  const name = item["наименование"] || "Без наименования";
  const type = item["тип"] || "—";
  const pn = item["парт номер"] || "—";
  const oem = item["oem парт номер"] || item["oem"] || "—";
  const qty = item["количество"] || "—";
  const price = item["цена"] || "—";
  const cur = item["валюта"] || "";
  const mfg = item["изготовитель"] || "—";

  const text = (item.text || "").trim();

  box.innerHTML = `
    <div class="item">
      <div class="itemBody">
        <div class="title">${esc(name)}</div>

        <div class="meta">
          <div><b>Тип:</b> ${esc(type)}</div>
          <div><b>Part №:</b> ${esc(pn)}</div>
          <div><b>OEM:</b> ${esc(oem)}</div>
          <div><b>Количество:</b> ${esc(qty)}</div>
          <div><b>Цена:</b> ${esc(price)} ${esc(cur)}</div>
          <div><b>Изготовитель:</b> ${esc(mfg)}</div>
        </div>

        ${text ? `<div class="pre" style="margin-top:10px;">${esc(text)}</div>` : ""}
      </div>
    </div>
  `;

  const imageUrl = item.image_url || "";
  if (imageUrl) {
    img.src = imageUrl;
    photoWrap.style.display = "block";
  } else {
    photoWrap.style.display = "none";
  }
}

async function loadItem() {
  const code = getCodeFromUrl();
  CURRENT.code = code;

  if (!code) {
    codeLine.textContent = "Код: —";
    setStatus("Не передан код детали", "error");
    box.innerHTML = "";
    issueBtn.disabled = true;
    return;
  }

  codeLine.textContent = `Код: ${code}`;
  if (envPill) envPill.textContent = "Загрузка…";
  setStatus("Загружаю карточку…", "muted");
  box.innerHTML = "";
  photoWrap.style.display = "none";
  issueBtn.disabled = true;

  // backend отдаёт: { ok:true, item:{...} }
  // используем /app/api/item (есть алиас /api/item, но так логичнее)
  const res = await fetch(`/app/api/item?code=${encodeURIComponent(code)}&user_id=${encodeURIComponent(userId())}`, {
    cache: "no-store"
  });
  const data = await safeJson(res);

  if (envPill) envPill.textContent = "Online";

  if (!res.ok || !data || !data.ok) {
    setStatus(data?.error || `Ошибка загрузки (${res.status})`, "error");
    return;
  }

  const item = data.item || null;
  if (!item) {
    setStatus("Данные по детали пустые", "error");
    return;
  }

  CURRENT.item = item;
  renderItem(item);

  setStatus("", "ok");
  issueBtn.disabled = false;
}

backBtn.addEventListener("click", () => {
  window.location.href = "/app";
});

issueBtn.addEventListener("click", async () => {
  const code = CURRENT.code;
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

  // у нас есть и /app/api/issue и /api/issue; используем /app/api/issue
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

  setStatus("Списание записано в История", "ok");
  alert("Списание записано в История");
});

loadItem();
