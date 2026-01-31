console.log("APP.JS LOADED cards_final_1");
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

async function apiGet(url){
  const r = await fetch(url, { method:"GET" });
  const t = await r.text();
  let j = null;
  try { j = JSON.parse(t); } catch(_e){ /* ignore */ }
  if (!r.ok) {
    const msg = j?.error || t || `HTTP ${r.status}`;
    throw new Error(msg);
  }
  return j ?? {};
}

async function apiPost(url, body){
  const r = await fetch(url, {
    method:"POST",
    headers: { "Content-Type":"application/json" },
    body: JSON.stringify(body ?? {})
  });
  const t = await r.text();
  let j = null;
  try { j = JSON.parse(t); } catch(_e){ /* ignore */ }
  if (!r.ok) {
    const msg = j?.error || t || `HTTP ${r.status}`;
    throw new Error(msg);
  }
  return j ?? {};
}

/* ===== Card renderer (ТВОЙ текущий шаблон, НЕ менял) ===== */
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

/* ===== Actions ===== */
async function doSearch(){
  const query = String(q.value || "").trim();
  if (!query) {
    list.innerHTML = "";
    cnt.textContent = "0";
    st.textContent = "";
    return;
  }

  st.textContent = "Поиск…";

  let data;
  try{
    const uid = userId();
    data = await apiGet(`/app/api/search?q=${encodeURIComponent(query)}&user_id=${encodeURIComponent(String(uid || 0))}`);
  }catch(e){
    list.innerHTML = `<div class="item is-enter"><div class="itemBody">Ошибка: ${esc(e.message)}</div></div>`;
    cnt.textContent = "0";
    st.textContent = "";
    return;
  }

  const items = Array.isArray(data?.items) ? data.items : [];
  cnt.textContent = String(items.length || 0);
  st.textContent = "";

  if (!items.length) {
    list.innerHTML = `<div class="item is-enter"><div class="itemBody">Ничего не найдено</div></div>`;
    return;
  }

  /* render */
  list.innerHTML = items.map(renderCard).join("");

  /* ===== FADE + SLIDE (FIX) ===== */
  requestAnimationFrame(() => {
    const cards = list.querySelectorAll(".item");
    console.log("cards animated:", cards.length);

    cards.forEach((el, i) => {
      el.style.animationDelay = `${i * 45}ms`; // лесенка
      el.classList.remove("is-enter");
      void el.offsetWidth; // форс-рефлоу
      el.classList.add("is-enter");
    });
  });

  /* copy code */
  document.querySelectorAll("[data-copy]").forEach(el => {
    el.addEventListener("click", () => {
      const v = el.getAttribute("data-copy") || "";
      try{
        navigator.clipboard?.writeText?.(v);
        tg?.HapticFeedback?.impactOccurred?.("light");
      }catch(_e){
        /* ignore */
      }
    }, { once:true });
  });

  /* issue buttons */
  document.querySelectorAll("[data-issue]").forEach(el => {
    el.addEventListener("click", async () => {
      const code = el.getAttribute("data-issue") || "";

      const qtyStr = prompt(`Сколько взять?\nКод: ${code}`, "1");
      if (qtyStr === null) return;

      const qty = Number(String(qtyStr).trim().replace(",", "."));
      if (!Number.isFinite(qty) || qty <= 0) {
        alert("Введите корректное количество");
        return;
      }

      const comment = prompt("Комментарий (необязательно):", "") ?? "";
      const ok = confirm(`Подтвердить списание?\nКод: ${code}\nКол-во: ${qty}\nКомментарий: ${comment || "—"}`);
      if (!ok) return;

      try{
        await apiPost("/app/api/issue", {
          user_id: userId(),
          code,
          qty,
          comment
        });
        tg?.HapticFeedback?.notificationOccurred?.("success");
      }catch(e){
        tg?.HapticFeedback?.notificationOccurred?.("error");
        alert(`Ошибка списания: ${e.message}`);
      }
    }, { once:true });
  });
}

/* ===== Events ===== */
btn?.addEventListener("click", doSearch);

clr?.addEventListener("click", () => {
  q.value = "";
  q.focus();
  list.innerHTML = "";
  cnt.textContent = "0";
  st.textContent = "";
});

q?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

/* Auto focus */
try{ q?.focus(); }catch(_e){ /* ignore */ }

