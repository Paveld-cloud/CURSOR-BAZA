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

function setStatus(text, kind="muted"){
  st.textContent = text || "";
  st.className = "small";
  if (kind === "bad") st.style.color = "rgba(255,120,120,.95)";
  else if (kind === "ok") st.style.color = "rgba(120,255,180,.95)";
  else st.style.color = "rgba(234,242,255,.70)";
}

/* ===== Card renderer ===== */
function renderCard(it){
  const name = esc(get(it, ["Наименование","наименование","Name","name","Title","title"]));
  const code = esc(get(it, ["Код","код","Code","code"]));
  const qty  = esc(get(it, ["Количество","количество","Qty","qty","Остаток","остаток"], "—"));
  const unit = esc(get(it, ["Ед.изм","ед.изм","Unit","unit"], ""));
  const typ  = esc(get(it, ["Тип","тип","Type","type"], ""));
  const img  = get(it, ["image_url","image","img","photo","Фото","фото"], "");

  const pillQty = (qty !== "—")
    ? `<span class="pill gold">Остаток: ${qty}${unit ? " "+unit : ""}</span>`
    : `<span class="pill">Остаток: —</span>`;

  const pillTyp = typ && typ !== "—"
    ? `<span class="pill">Тип: ${typ}</span>`
    : "";

  const thumb = img
    ? `<img src="${esc(img)}" alt="img" loading="lazy" />`
    : `<div class="small" style="opacity:.7">нет фото</div>`;

  return `
  <div class="item" data-code="${code}" data-name="${name}">
    <div class="hd">
      <div>
        <div class="ttl">${name}</div>
        <div class="sub">Код: <span data-copy="${code}" style="text-decoration:underline; cursor:pointer">${code}</span></div>
      </div>
      <div class="badge">MG</div>
    </div>

    <div class="bd">
      <div class="thumb">${thumb}</div>
      <div class="meta">
        <div class="row">
          ${pillQty}
          ${pillTyp}
        </div>
        <div class="row">
          <span class="pill">Пользователь: ${esc(userName() || String(userId() || ""))}</span>
        </div>
      </div>
    </div>

    <div class="ft">
      <button class="btn primary" data-issue="${code}">Взять деталь</button>
      <a class="btn" href="/app/item?code=${encodeURIComponent(code)}&user_id=${encodeURIComponent(String(userId() || 0))}">Открыть</a>
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
    setStatus("Введите запрос", "muted");
    return;
  }

  setStatus("Поиск…", "muted");

  let data;
  try{
    const uid = userId();
    data = await apiGet(`/app/api/search?q=${encodeURIComponent(query)}&user_id=${encodeURIComponent(String(uid || 0))}`);
  }catch(e){
    list.innerHTML = "";
    cnt.textContent = "0";
    setStatus(`Ошибка: ${e.message}`, "bad");
    return;
  }

  const items = Array.isArray(data?.items) ? data.items : [];
  cnt.textContent = String(items.length || 0);

  if (!items.length){
    list.innerHTML = "";
    setStatus("Ничего не найдено", "muted");
    return;
  }

  setStatus("Готово", "ok");

  /* render */
  list.innerHTML = items.map(renderCard).join("");

  /* ===== FADE + SLIDE (ГАРАНТИРОВАННО) ===== */
  requestAnimationFrame(() => {
    const cards = list.querySelectorAll(".item");
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
        setStatus("Скопировано", "ok");
      }catch(_e){
        /* ignore */
      }
    }, { once:true });
  });

  /* issue buttons */
  document.querySelectorAll("[data-issue]").forEach(el => {
    el.addEventListener("click", async () => {
      const code = el.getAttribute("data-issue") || "";
      const card = el.closest(".item");
      const name = card?.getAttribute("data-name") || "";

      const qtyStr = prompt(`Сколько взять?\n${name}\nКод: ${code}`, "1");
      if (qtyStr === null) return;
      const qty = toNum(qtyStr);
      if (!qty || qty <= 0) {
        alert("Введите корректное количество");
        return;
      }

      const comment = prompt("Комментарий (необязательно):", "") ?? "";
      const ok = confirm(`Подтвердить списание?\n${name}\nКод: ${code}\nКол-во: ${qty}\nКомментарий: ${comment || "—"}`);
      if (!ok) return;

      try{
        await apiPost("/app/api/issue", {
          user_id: userId(),
          user_name: userName(),
          name,
          code,
          qty,
          comment
        });
        tg?.HapticFeedback?.notificationOccurred?.("success");
        setStatus("Списание сохранено", "ok");
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
  setStatus("", "muted");
});

q?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

/* Auto focus */
try{ q?.focus(); }catch(_e){ /* ignore */ }


