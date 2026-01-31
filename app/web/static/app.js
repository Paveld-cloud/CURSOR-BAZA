/* BAZA MG — app.js (Search + Render in approved UI) */

const tg = window.Telegram?.WebApp;
try { tg?.expand?.(); } catch (_) {}

function getUserId() {
  return tg?.initDataUnsafe?.user?.id || 0;
}

function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function pick(obj, keys, def = "") {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null) {
      const t = String(v).trim();
      if (t !== "") return t;
    }
  }
  return def;
}

function normCode(code) {
  return String(code || "").trim();
}

function buildItemUrl(code) {
  // У тебя может быть /item (route) или item.html (static). Делаем оба варианта.
  const c = encodeURIComponent(code);
  // 1) Предпочтительный роут (если есть)
  return `/item?code=${c}`;
}

function buildItemUrlFallback(code) {
  const c = encodeURIComponent(code);
  return `/item.html?code=${c}`;
}

async function apiGetJSON(url) {
  const r = await fetch(url, { method: "GET" });
  const txt = await r.text();
  let json = null;
  try { json = JSON.parse(txt); } catch (_) {}
  if (!r.ok) {
    const msg = json?.error || txt || `HTTP ${r.status}`;
    throw new Error(msg);
  }
  return json ?? {};
}

/* ===== DOM discovery (works with UI Demo / Index variants) ===== */
const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");

// Results header + count badge
const resultsHead = document.querySelector(".resultsHead");
const countBadge =
  document.querySelector(".resultsHead .count") ||
  document.querySelector(".count");

// Container where cards will be injected
function ensureResultsContainer() {
  // try common ids/classes
  let container =
    document.getElementById("resultsList") ||
    document.getElementById("list") ||
    document.querySelector(".resultsList") ||
    document.querySelector("[data-results]");

  if (container) return container;

  // create under the results section
  const resultsSection = document.querySelector(".results") || document.querySelector("main");
  if (!resultsSection) return null;

  container = document.createElement("div");
  container.id = "resultsList";
  container.style.display = "flex";
  container.style.flexDirection = "column";
  container.style.gap = "12px";

  // insert after results head if exists
  if (resultsHead && resultsHead.parentElement) {
    resultsHead.parentElement.appendChild(container);
  } else {
    resultsSection.appendChild(container);
  }

  return container;
}

const resultsList = ensureResultsContainer();

/* ===== UI render ===== */
function renderCard(item, userName) {
  const code = pick(item, ["code", "Код", "код", "part_code", "partCode", "код детали"], "");
  const codeNorm = normCode(code);

  const qty = pick(item, ["qty", "остаток", "Остаток", "количество", "Количество"], "—");
  const name = pick(item, ["name", "наименование", "Наименование", "title", "Название"], "Без наименования");

  const type = pick(item, ["type", "тип", "Тип"], "—");
  const partNo = pick(item, ["part_no", "part", "парт номер", "Парт номер", "Part №", "part_number"], "—");
  const oem = pick(item, ["oem", "OEM", "oem_part", "OEM парт номер", "oem парт номер"], "—");
  const category = pick(item, ["category", "категория", "Категория"], "—");

  const imageUrl = pick(item, ["image_url", "image", "photo", "img", "imageUrl"], "");

  const qtyNum = Number(String(qty).replace(",", "."));
  const qtyOk = Number.isFinite(qtyNum) ? (qtyNum > 0) : true;

  const itemUrl = buildItemUrl(codeNorm);
  const itemUrlFallback = buildItemUrlFallback(codeNorm);

  return `
    <article class="itemCard" data-code="${esc(codeNorm)}">
      <div class="itemTop">
        <div class="kv">
          <span class="k">КОД:</span>
          <span class="v monoPill">${esc(codeNorm || "—")}</span>
        </div>

        <div class="kv">
          <span class="k">ОСТАТОК:</span>
          <span class="v monoPill ${qtyOk ? "monoPill--ok" : ""}">${esc(qty)}</span>
        </div>
      </div>

      <div class="itemTitle">${esc(name)}</div>

      <div class="itemBody">
        <div class="thumb">
          ${
            imageUrl
              ? `<img src="${esc(imageUrl)}" alt="Фото" loading="lazy" onerror="this.style.display='none'; this.parentElement.classList.add('noimg');">`
              : `<div class="noimgBox">Фото</div>`
          }
        </div>

        <div class="meta">
          <div class="metaRow">
            <span class="tag"><span class="tagDot"></span>Тип: ${esc(type)}</span>
          </div>

          <div class="metaRow">
            <span class="tag"><span class="tagDot tagDot--cyan"></span>Пользователь: ${esc(userName || "—")}</span>
          </div>

          <div class="metaList">
            <div><span class="mK">Part №:</span> <span class="mV mono">${esc(partNo)}</span></div>
            <div><span class="mK">OEM:</span> <span class="mV mono">${esc(oem)}</span></div>
            <div><span class="mK">Категория:</span> <span class="mV">${esc(category)}</span></div>
          </div>
        </div>
      </div>

      <div class="actions">
        <button class="btn primary wide" data-take>ВЗЯТЬ ДЕТАЛЬ</button>
        <a class="btn wide" data-open href="${esc(itemUrl)}">Открыть</a>
      </div>

      <div style="display:none" data-fallback="${esc(itemUrlFallback)}"></div>
    </article>
  `;
}

function setCount(n) {
  if (countBadge) countBadge.textContent = String(n);
}

function showInfoCard(text) {
  if (!resultsList) return;
  resultsList.innerHTML = `
    <section class="card" style="padding:14px;">
      <div style="color: rgba(234,242,255,.80); font-size:13px; line-height:1.4;">
        ${esc(text)}
      </div>
    </section>
  `;
  setCount(0);
}

/* ===== Actions binding ===== */
function bindCardActions() {
  if (!resultsList) return;

  // Open link fallback if /item route isn't available
  resultsList.querySelectorAll("a[data-open]").forEach((a) => {
    a.addEventListener("click", async (e) => {
      // if route /item not found, browser will show 404; fallback by prechecking is heavy.
      // We'll do a simple trick: if Telegram WebApp and link can't open, user will come back.
      // Keep as is. If you want, we'll hard-switch to item.html later.
    }, { once: true });
  });

  // Take button stub for now
  resultsList.querySelectorAll("button[data-take]").forEach((b) => {
    b.addEventListener("click", () => {
      const card = b.closest("[data-code]");
      const code = card?.getAttribute("data-code") || "";
      // Заглушка — списание подключим следующим шагом, не ломая UI.
      alert(`Списание подключим следующим шагом.\nКод: ${code}`);
    }, { once: true });
  });
}

/* ===== Search ===== */
async function doSearch() {
  const query = String(q?.value || "").trim();
  if (!query) {
    showInfoCard("Введите запрос для поиска.");
    return;
  }

  showInfoCard("Поиск…");

  const uid = getUserId();
  const url = `/app/api/search?q=${encodeURIComponent(query)}&user_id=${encodeURIComponent(String(uid || 0))}`;

  let data;
  try {
    data = await apiGetJSON(url);
  } catch (e) {
    showInfoCard(`Ошибка поиска: ${e.message}`);
    return;
  }

  const items = Array.isArray(data?.items) ? data.items : Array.isArray(data) ? data : [];
  setCount(items.length);

  if (!items.length) {
    showInfoCard("Ничего не найдено.");
    return;
  }

  const userName =
    tg?.initDataUnsafe?.user?.first_name ||
    tg?.initDataUnsafe?.user?.username ||
    "—";

  resultsList.innerHTML = items.map((it) => renderCard(it, userName)).join("");
  bindCardActions();
}

/* ===== Wire up ===== */
btn?.addEventListener("click", doSearch);

clr?.addEventListener("click", () => {
  if (q) q.value = "";
  showInfoCard("Введите запрос для поиска.");
  try { q?.focus?.(); } catch (_) {}
});

q?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

// initial state
if (resultsList) showInfoCard("Введите запрос для поиска.");

