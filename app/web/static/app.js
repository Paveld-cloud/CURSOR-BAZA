const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

/* ===== DOM ===== */
const q    = document.getElementById("q");
const btn  = document.getElementById("btn");
const clr  = document.getElementById("clr");
const st   = document.getElementById("st");
const cnt  = document.getElementById("cnt");
const list = document.getElementById("list");

/* ===== Helpers ===== */
function userId() {
  return tg?.initDataUnsafe?.user?.id || 0;
}

function userName() {
  const u = tg?.initDataUnsafe?.user;
  if (!u) return "";
  const fn = (u.first_name || "").trim();
  const ln = (u.last_name || "").trim();
  return (fn + " " + ln).trim() || (u.username ? "@"+u.username : "");
}

function esc(s){
  return String(s ?? "")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;");
}

function get(it, keys, def="—"){
  for (const k of keys){
    const v = it?.[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") {
      return String(v).trim();
    }
  }
  return def;
}

function toNum(x){
  const s = String(x ?? "").trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

async function safeJson(res){
  try { return await res.json(); } catch { return null; }
}

function clearUI(){
  q.value = "";
  if (st) st.textContent = "";
  if (cnt) cnt.textContent = "";
  if (list) list.innerHTML = "";
  q.focus();
  tg?.HapticFeedback?.impactOccurred("light");
}

/* ===== Render card ===== */
function renderCard(it){
  const codeShow = get(it, ["код","code"], "—");
  const codeSend = get(it, ["код","code"], "").toLowerCase();
  const name = get(it, ["наименование","name"], "Без наименования");

  const type  = get(it, ["тип","type"]);
  const part  = get(it, ["парт номер","part","part_number"]);
  const oem   = get(it, ["oem","oem парт номер","OEM парт номер"]);
  const qty   = get(it, ["количество","остаток","qty"]);
  const price = get(it, ["цена","price"]);
  const cur   = get(it, ["валюта","currency"]);
  const mfg   = get(it, ["изготовитель","manufacturer"]);

  const img = get(it, ["image_url","image","photo"], "");

  return `
    <div class="item">
      <div class="itemPhoto ${img ? "" : "noimg"}">
        ${
          img
            ? `<img class="photo" src="${esc(img)}" alt="Фото" loading="lazy">`
            : `<div class="noPhoto">Фото не найдено</div>`
        }
      </div>

      <div class="itemBody">
        <div class="codeLine">
          <span>КОД: <b>${esc(codeShow)}</b></span>
          <span>ОСТАТОК: <b>${esc(qty)}</b></span>
        </div>

        <div class="title">${esc(name)}</div>

        <div class="meta">
          <div><b>Тип:</b> ${esc(type)}</div>
          <div><b>Part №:</b> ${esc(part)}</div>
          <div><b>OEM:</b> ${esc(oem)}</div>
          <div><b>Цена:</b> ${esc(price)} ${esc(cur)}</div>
          <div><b>Изготовитель:</b> ${esc(mfg)}</div>
        </div>

        <div class="btnRow">
          <button class="btn" data-issue="${esc(codeSend)}">📦 Взять деталь</button>
          <button class="btn ghost" data-copy="${esc(codeShow)}">📋 Код</button>
        </div>
      </div>
    </div>
  `;
}

/* ===== Search ===== */
async function doSearch(){
  const text = (q.value || "").trim();
  if (!text) {
    if (st) st.textContent = "Введите запрос";
    return;
  }

  if (st) st.textContent = "Ищу…";
  if (cnt) cnt.textContent = "";
  list.innerHTML = "";

  let res, data;
  try {
    res = await fetch(
      `/app/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`,
      { cache: "no-store" }
    );
    data = await safeJson(res);
  } catch {
    if (st) st.textContent = "Ошибка сети";
    return;
  }

  if (!res.ok || !data || !data.ok) {
    if (st) st.textContent = data?.error || "Ошибка поиска";
    return;
  }

  const items = data.items || [];
  if (st) st.textContent = `Найдено: ${items.length}`;
  if (cnt) cnt.textContent = items.length;

  if (!items.length) {
    list.innerHTML = `<div class="item"><div class="itemBody">Ничего не найдено</div></div>`;
    return;
  }

  /* render */
  list.innerHTML = items.map(renderCard).join("");

  /* ===== FADE + SLIDE (ГАРАНТИРОВАННО) ===== */
  const cards = list.querySelectorAll(".item");
  console.log("cards animated:", cards.length);

  cards.forEach((el, i) => {
    el.style.animationDelay = `${i * 45}ms`; // лесенка
    el.classList.remove("is-enter");
    void el.offsetWidth; // форс-рефлоу
    el.classList.add("is-enter");
  });

  /* copy code */
  document.querySelectorAll("[data-copy]").forEach(btn => {
    btn.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(btn.dataset.copy);
        tg?.HapticFeedback?.notificationOccurred("success");
        if (st) st.textContent = "Код скопирован ✅";
      } catch {
        if (st) st.textContent = "Ошибка копирования";
      }
    });
  });

  /* issue */
  document.querySelectorAll("[data-issue]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const code = btn.dataset.issue;
      if (!code) return;

      const qtyStr = prompt("Сколько списать?");
      if (!qtyStr) return;

      const qtyNum = toNum(qtyStr);
      if (!qtyNum || qtyNum <= 0) {
        alert("Введите корректное количество");
        return;
      }

      const comment = (prompt("Комментарий") || "").trim();
      if (!confirm(`Подтвердить списание?\nКод: ${code}\nКол-во: ${qtyNum}`)) return;

      let r, out;
      try {
        r = await fetch("/app/api/issue", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: userId(),
            name: userName(),
            code,
            qty: qtyNum,
            comment
          })
        });
        out = await safeJson(r);
      } catch {
        alert("Ошибка сети");
        return;
      }

      if (!r.ok || !out || !out.ok) {
        alert(out?.error || "Ошибка списания");
        return;
      }

      alert("✅ Списание записано");
    });
  });
}

/* ===== Events ===== */
btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", e => { if (e.key === "Enter") doSearch(); });
clr?.addEventListener("click", clearUI);


