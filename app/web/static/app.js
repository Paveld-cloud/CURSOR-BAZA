// app/web/static/app.js

const $ = (id) => document.getElementById(id);

const elInput = $("searchInput");
const elBtnSearch = $("searchBtn");
const elBtnClear = $("clearBtn");
const elResults = $("results");
const elError = $("errorLine"); // если нет - будет просто игнор

function setError(text) {
  if (elError) {
    elError.textContent = text || "";
    elError.style.display = text ? "block" : "none";
  }
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderEmpty() {
  elResults.innerHTML = `<div class="empty">Ничего не найдено</div>`;
}

function renderCards(items) {
  elResults.innerHTML = "";

  items.forEach((item) => {
    const code = escapeHtml(item.code || "");
    const name = escapeHtml(item.name || "");
    const type = escapeHtml(item.type || "");
    const part = escapeHtml(item.part || "");
    const oem = escapeHtml(item.oem || "");
    const qty = escapeHtml(item.qty || "");
    const price = escapeHtml(item.price || "");
    const currency = escapeHtml(item.currency || "");
    const image = item.image || "";

    const card = document.createElement("div");
    card.className = "card";

    // --- верх: картинка или "без фото"
    if (image) {
      const img = document.createElement("img");
      img.className = "img";
      img.src = image;
      img.alt = "Фото";
      img.onerror = () => {
        // если ссылка битая — показываем "без фото"
        img.remove();
        const no = document.createElement("div");
        no.className = "no-photo";
        no.textContent = "без фото";
        card.prepend(no);
      };
      card.appendChild(img);
    } else {
      const no = document.createElement("div");
      no.className = "no-photo";
      no.textContent = "без фото";
      card.appendChild(no);
    }

    // --- тело карточки
    const body = document.createElement("div");
    body.className = "card-body";
    body.innerHTML = `
      <div class="pill-row">
        <div class="pill">Код <b>${code}</b></div>
        <div class="pill green">Остаток <b>${qty}</b></div>
      </div>

      <div class="title">${name}</div>

      ${type ? `<div class="row"><span class="k">Тип:</span> <span class="v">${type}</span></div>` : ""}
      ${part ? `<div class="row"><span class="k">Part №:</span> <span class="v">${part}</span></div>` : ""}
      ${oem ? `<div class="row"><span class="k">OEM:</span> <span class="v">${oem}</span></div>` : ""}

      ${(price || currency) ? `<div class="row"><span class="k">Цена:</span> <span class="v">${price} ${currency}</span></div>` : ""}

      <div class="actions">
        <button class="btn primary" data-code="${code}">📦 Взять</button>
        <button class="btn ghost" data-code="${code}">ℹ️ Описание</button>
      </div>
    `;
    card.appendChild(body);

    elResults.appendChild(card);
  });
}

async function doSearch() {
  const q = (elInput.value || "").trim();
  setError("");

  if (!q) {
    renderEmpty();
    return;
  }

  // user_id иногда передаёшь — оставим
  const userId = window.Telegram?.WebApp?.initDataUnsafe?.user?.id || "";
  const url = `/api/search?q=${encodeURIComponent(q)}${userId ? `&user_id=${encodeURIComponent(userId)}` : ""}`;

  let res;
  try {
    res = await fetch(url, { method: "GET", cache: "no-store" });
  } catch (e) {
    setError("Ошибка сети (fetch)");
    renderEmpty();
    return;
  }

  // Статус НЕ считаем ошибкой, пока res.ok
  if (!res.ok) {
    setError(`Ошибка поиска (${res.status})`);
    renderEmpty();
    return;
  }

  // JSON парсинг
  let data;
  try {
    data = await res.json();
  } catch (e) {
    setError("Ошибка ответа (JSON)");
    renderEmpty();
    return;
  }

  // сервер может вернуть [] или {items:[]}
  const items = Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : []);

  if (!items.length) {
    setError(""); // не ошибка
    renderEmpty();
    return;
  }

  renderCards(items);
}

function clearAll() {
  elInput.value = "";
  setError("");
  renderEmpty();
}

// --- bindings ---
if (elBtnSearch) elBtnSearch.addEventListener("click", doSearch);
if (elBtnClear) elBtnClear.addEventListener("click", clearAll);

if (elInput) {
  elInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") doSearch();
  });
}

// стартовый экран
renderEmpty();

