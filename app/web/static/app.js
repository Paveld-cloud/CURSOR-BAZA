const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

const q   = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");
const st  = document.getElementById("st");
const cnt = document.getElementById("cnt");
const list = document.getElementById("list");

function userId() { return tg?.initDataUnsafe?.user?.id || 0; }
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
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v).trim();
  }
  return def;
}

function clearUI() {
  q.value = "";
  if (st) st.textContent = "";
  if (cnt) cnt.textContent = "";
  if (list) list.innerHTML = "";
  q.focus();
  if (tg?.HapticFeedback) tg.HapticFeedback.impactOccurred("light");
}

function toNum(x){
  const s = String(x ?? "").trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

async function safeJson(res){
  try { return await res.json(); } catch { return null; }
}

function renderCard(it){
  const code = get(it, ["код","code"], "").toLowerCase();
  const codeShow = get(it, ["код","code"], "—");
  const name = get(it, ["наименование","name"], "Без наименования");

  const type = get(it, ["тип","type"], "—");
  const part = get(it, ["парт номер","part","part_number"], "—");
  const oem  = get(it, ["oem","oem парт номер","OEM парт номер"], "—");
  const qty  = get(it, ["количество","остаток","qty"], "—");
  const price= get(it, ["цена","price"], "—");
  const cur  = get(it, ["валюта","currency"], "");
  const mfg  = get(it, ["изготовитель","manufacturer"], "—");

  const img = get(it, ["image_url","image","photo"], "");

  return `
    <div class="item">
      <div class="itemPhoto ${img ? "" : "noimg"}">
        ${img ? `<img class="photo" src="${esc(img)}" alt="Фото" loading="lazy" />`
              : `<div class="noPhoto">Фото не найдено</div>`}
      </div>

      <div class="itemBody">
        <div class="codeLine">
          <span>КОД: <b>${esc(codeShow)}</b></span>
          <span>ОСТАТОК: <b>${esc(qty)}</b></span>
        </div>

        <div class="title">${esc(name)}</div>

        <!-- ПОЛНОЕ ОПИСАНИЕ СРАЗУ В КАРТОЧКЕ -->
        <div class="meta">
          <div><b>Тип:</b> ${esc(type)}</div>
          <div><b>Part №:</b> ${esc(part)}</div>
          <div><b>OEM:</b> ${esc(oem)}</div>
          <div><b>Цена:</b> ${esc(price)} ${esc(cur)}</div>
          <div><b>Изготовитель:</b> ${esc(mfg)}</div>
        </div>

        <div class="btnRow">
          <button class="btn" data-issue="${esc(code)}">📦 Взять деталь</button>
          <button class="btn ghost" data-copy="${esc(codeShow)}">📋 Код</button>
        </div>
      </div>
    </div>
  `;
}

async function doSearch(){
  const text = (q.value||"").trim();
  if(!text){
    if (st) st.textContent = "Введите запрос";
    if (cnt) cnt.textContent = "";
    return;
  }

  if (st) st.textContent = "Ищу...";
  if (cnt) cnt.textContent = "";
  if (list) list.innerHTML = "";

  const url = `/app/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;

  let res, data;
  try {
    res = await fetch(url, { cache: "no-store" });
    data = await safeJson(res);
  } catch (e) {
    if (st) st.textContent = "Ошибка поиска. Проверьте соединение.";
    return;
  }

  if(!res.ok || !data || !data.ok){
    if (st) st.textContent = data?.error || "Ошибка поиска";
    return;
  }

  // backend у тебя отдаёт items
  const items = data.items || [];
  if (st) st.textContent = `Найдено: ${items.length}`;
  if (cnt) cnt.textContent = items.length ? String(items.length) : "";

  if(!items.length){
    if (list) list.innerHTML = `<div class="item"><div class="itemBody">Ничего не найдено</div></div>`;
    return;
  }

  // рендер
  if (list) list.innerHTML = items.map(renderCard).join("");
  // fade + slide появление карточек
const cards = list.querySelectorAll(".item");
cards.forEach((el, i) => {
  el.classList.remove("is-enter");
  el.style.animationDelay = `${i * 35}ms`; // лесенка (можно 0 если не надо)
  el.classList.add("is-enter");
});

  // авто-адаптив фото
  document.querySelectorAll(".photo").forEach(imgEl => {
    imgEl.addEventListener("load", () => {
      const w = imgEl.naturalWidth || 1;
      const h = imgEl.naturalHeight || 1;
      const ratio = w / h;
      if (ratio < 0.85) imgEl.classList.add("fit-contain");
      else imgEl.classList.add("fit-cover");
    }, { once: true });
  });

  // копирование кода
  document.querySelectorAll("[data-copy]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      const codeText = b.getAttribute("data-copy") || "";
      if (!codeText) return;
      try{
        await navigator.clipboard.writeText(codeText);
        if (tg?.HapticFeedback) tg.HapticFeedback.notificationOccurred("success");
        if (st) st.textContent = "Код скопирован ✅";
      }catch{
        if (st) st.textContent = "Не удалось скопировать код";
      }
    });
  });

  // списание
  document.querySelectorAll("[data-issue]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      const code = b.getAttribute("data-issue");
      if(!code) return;

      const qtyStr = prompt("Сколько списать? (пример: 1 или 2.5)");
      if(!qtyStr) return;

      const qtyNum = toNum(qtyStr);
      if (qtyNum === null || qtyNum <= 0){
        alert("Введите корректное количество.");
        return;
      }

      const comment = (prompt("Комментарий (пример: OP-1100 авария, замена датчика)") || "").trim();

      // подтверждение (как ты любишь — Да/Нет)
      const ok = confirm(`Подтвердить списание?\nКод: ${code}\nКол-во: ${qtyNum}`);
      if (!ok) return;

      const payload = {
        user_id: userId(),
        name: userName(),
        code: code,
        qty: qtyNum,
        comment: comment
      };

      let r, out;
      try {
        r = await fetch("/app/api/issue", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(payload)
        });
        out = await safeJson(r);
      } catch {
        alert("Ошибка сети при списании");
        return;
      }

      if(!r.ok || !out || !out.ok){
        alert(out?.error || "Ошибка списания");
        return;
      }

      alert("✅ Списание записано в История");
    });
  });
}

// события
btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", e=>{ if(e.key==="Enter") doSearch(); });
clr?.addEventListener("click", clearUI);

