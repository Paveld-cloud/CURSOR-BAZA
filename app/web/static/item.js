const tg = window.Telegram?.WebApp;

if (tg) {
  tg.expand();
  tg.setHeaderColor?.('#121212');
  tg.setBackgroundColor?.('#0b1220');
  tg.ready();
}

const codeLine  = document.getElementById("codeLine");
const envPill   = document.getElementById("envPill");
const statusEl  = document.getElementById("status");
const box       = document.getElementById("box");
const img       = document.getElementById("img");
const photoWrap = document.getElementById("photoWrap");
const backBtn   = document.getElementById("back");
const issueBtn  = document.getElementById("issue");

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

/** Унифицированный возврат */
function goBack() {
  // если пришли со страницы /app — у истории будет куда вернуться
  if (window.history.length > 1) {
    window.history.back();
    return;
  }
  // фолбэк — всегда на главную мини-аппы
  window.location.href = "/app/";
}

let CURRENT = { code: "", row: null };

async function loadItem() {
  const code = getCodeFromUrl();
  CURRENT.code = code;

  if (!code) {
    codeLine.textContent = "Код: —";
    setStatus("Не передан код детали", "error");
    box.innerHTML = "";
    if (issueBtn) issueBtn.disabled = true;
    return;
  }

  codeLine.textContent = `Код: ${code}`;
  if (envPill) envPill.textContent = "Загрузка…";
  setStatus("Загружаю карточку…", "muted");
  box.innerHTML = "";
  if (photoWrap) photoWrap.style.display = "none";
  if (issueBtn) issueBtn.disabled = true;

  // ВАЖНО: унифицировали пути -> /app/api/...
  const res = await fetch(
    `/app/api/item?code=${encodeURIComponent(code)}&user_id=${encodeURIComponent(userId())}`,
    { cache: "no-store" }
  );
  const data = await safeJson(res);

  if (envPill) envPill.textContent = "Online";

  if (!res.ok || !data || !data.ok) {
    setStatus(data?.error || `Ошибка загрузки (${res.status})`, "error");
    return;
  }

  const row = data.row || data.item || null;
  CURRENT.row = row;

  // если backend отдаёт card_html — показываем как есть (это уже готовая карточка)
  if (data.card_html) {
    box.innerHTML = `
      <div class="item">
        <div class="item__sub">${data.card_html}</div>
      </div>
    `;
  } else if (row) {
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
  if (data.image_url && img && photoWrap) {
    img.src = data.image_url;
    photoWrap.style.display = "block";
  }

  setStatus("", "ok");
  if (issueBtn) issueBtn.disabled = false;
}

/* Кнопка "Назад" в интерфейсе */
if (backBtn) backBtn.addEventListener("click", goBack);

/* Системная кнопка Telegram BackButton (самое стабильное) */
if (tg?.BackButton) {
  tg.BackButton.show();
  tg.BackButton.onClick(goBack);
}

if (issueBtn) {
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

    // унифицированный путь -> /app/api/issue
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
}

loadItem();
