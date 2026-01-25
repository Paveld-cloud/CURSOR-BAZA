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

let CURRENT = { code: "", row: null };

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

  // ВАЖНО: нужен endpoint /api/item?code=...
  const res = await fetch(`/api/item?code=${encodeURIComponent(code)}&user_id=${encodeURIComponent(userId())}`);
  const data = await safeJson(res);

  if (envPill) envPill.textContent = "Online";

  if (!res.ok || !data || !data.ok) {
    setStatus(data?.error || `Ошибка загрузки (${res.status})`, "error");
    return;
  }

  const row = data.row || null;
  CURRENT.row = row;

  // card_html если есть — рендерим как HTML (это у тебя уже формируется)
  if (data.card_html) {
    box.innerHTML = `
      <div class="item">
        <div class="item__sub">${data.card_html}</div>
      </div>
    `;
  } else if (row) {
    // запасной вариант — рендер из row
    box.innerHTML = `
      <div class="item">
        <div class="item__title">${esc(row["наименование"] || "Без наименования")}</div>
        <div class="item__sub">
          <div><b>Тип:</b> ${esc(row["тип"] || "—")}</div>
          <div><b>Part №:</b> ${esc(row["парт номер"] || "—")}</div>
          <div><b>OEM:</b> ${esc(row["oem парт номер"] || row["oem"] || "—")}</div>
          <div><b>Количество:</b> ${esc(row["количество"] || "—")}</div>
          <div><b>Цена:</b> ${esc(row["цена"] || "—")} ${esc(row["валюта"] || "")}</div>
          <div><b>Изготовитель:</b> ${esc(row["изготовитель"] || "—")}</div>
        </div>
      </div>
    `;
  } else {
    setStatus("Данные по детали пустые", "error");
    return;
  }

  // фото
  if (data.image_url) {
    img.src = data.image_url;
    photoWrap.style.display = "block";
  }

  setStatus("", "ok");
  issueBtn.disabled = false;
}

backBtn.addEventListener("click", () => {
  // возвращаемся в /app (главная mini app)
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

  const res = await fetch("/api/issue", {
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

loadItem();
