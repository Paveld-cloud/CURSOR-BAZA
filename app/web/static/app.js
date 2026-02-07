/* BAZA MG — app.js (Search + Full fields render) */

const tg = window.Telegram?.WebApp;
try { tg?.expand?.(); } catch (_) {}

function getUserId(){ return tg?.initDataUnsafe?.user?.id || 0; }
function getUserName(){
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
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}

function pick(obj, keys, def=""){
  for (const k of keys){
    const v = obj?.[k];
    if (v !== undefined && v !== null){
      const t = String(v).trim();
      if (t !== "") return t;
    }
  }
  return def;
}

function normCode(code){ return String(code||"").trim(); }

async function apiGetJSON(url){
  const r = await fetch(url, { method:"GET" });
  const txt = await r.text();
  let json = null;
  try{ json = JSON.parse(txt); }catch(_){}
  if(!r.ok){
    const msg = json?.error || txt || `HTTP ${r.status}`;
    throw new Error(msg);
  }
  return json ?? {};
}

/* DOM */
const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");

const countBadge =
  document.querySelector(".resultsHead .count") ||
  document.querySelector(".count");

const resultsList =
  document.getElementById("list") ||
  document.querySelector("[data-results]") ||
  document.querySelector(".resultsList");

function setCount(n){ if (countBadge) countBadge.textContent = String(n); }

function showInfo(text){
  if(!resultsList) return;
  resultsList.innerHTML = `
    <section class="card" style="padding:14px;">
      <div style="color: rgba(234,242,255,.80); font-size:13px; line-height:1.4;">
        ${esc(text)}
      </div>
    </section>
  `;
  setCount(0);
}

/* Автовытаскиваем все доп.поля */
const SKIP_KEYS = new Set([
  "code","Код","код","part_code","partCode","код детали",
  "qty","остаток","Остаток","количество","Количество",
  "name","наименование","Наименование","title","Название",
  "type","тип","Тип",
  "part_no","part","парт номер","Парт номер","Part №","part_number",
  "oem","OEM","oem_part","OEM парт номер","oem парт номер",
  "category","категория","Категория",
  "image","photo","img","image_url","imageUrl",
  "user","user_id","username","first_name","last_name",
]);

function extraFields(obj){
  if(!obj || typeof obj !== "object") return [];
  const out = [];
  for (const [k,v] of Object.entries(obj)){
    if (SKIP_KEYS.has(k)) continue;
    if (v === null || v === undefined) continue;
    const s = String(v).trim();
    if (!s) continue;

    // убираем слишком длинные поля в одну строку — покажем обрезанно
    out.push([k, s]);
  }
  return out;
}

function buildItemUrl(code){
  const c = encodeURIComponent(code);
  // если у тебя есть роут /item — ок, если нет — поменяем на item.html позже
  return `/item?code=${c}`;
}

function renderCard(item){
  const code = normCode(pick(item, ["code","Код","код","part_code","partCode","код детали"], ""));
  const qty  = pick(item, ["qty","остаток","Остаток","количество","Количество"], "—");
  const name = pick(item, ["name","наименование","Наименование","title","Название"], "Без наименования");

  const type = pick(item, ["type","тип","Тип"], "—");
  const partNo = pick(item, ["part_no","part","парт номер","Парт номер","Part №","part_number"], "—");
  const oem = pick(item, ["oem","OEM","oem_part","OEM парт номер","oem парт номер"], "—");
  const category = pick(item, ["category","категория","Категория"], "—");
  const mfg = pick(item, ["mfg","manufacturer","производитель","Производитель","brand","бренд"], "—");

  const imageUrl = pick(item, ["image_url","image","photo","img","imageUrl"], "");

  const userName = getUserName() || "—";

  const extras = extraFields(item);
  const extrasHtml = extras.length ? `
    <div class="metaList" style="margin-top:10px;">
      <div style="font-weight:900; color: rgba(234,242,255,.85); border-bottom:1px dashed rgba(255,255,255,.10); padding-bottom:6px; margin-bottom:6px;">
        Доп. параметры
      </div>
      ${extras.slice(0, 12).map(([k,v])=>`
        <div><span class="mK">${esc(k)}:</span> <span class="mV">${esc(v)}</span></div>
      `).join("")}
      ${extras.length > 12 ? `
        <div><span class="mK">…</span><span class="mV">ещё ${extras.length-12}</span></div>
      ` : ""}
    </div>
  ` : "";

  return `
    <article class="itemCard" data-code="${esc(code)}">
      <div class="itemTop">
        <div class="kv">
          <span class="k">КОД:</span>
          <span class="v monoPill">${esc(code || "—")}</span>
        </div>

        <div class="kv">
          <span class="k">ОСТАТОК:</span>
          <span class="v monoPill monoPill--ok">${esc(qty)}</span>
        </div>
      </div>

      <div class="itemTitle">${esc(name)}</div>

      <div class="itemBody">
        <div class="thumb">
          ${
            imageUrl
              ? `<img src="${esc(imageUrl)}" alt="Фото" loading="lazy">`
              : `<div class="noimgBox" style="display:flex;align-items:center;justify-content:center;height:100%;color:rgba(234,242,255,.55);">Нет фото</div>`
          }
        </div>

        <div class="meta">
          <div class="metaRow">
            <span class="tag"><span class="tagDot"></span>Тип: ${esc(type)}</span>
          </div>

          <div class="metaRow">
            <span class="tag"><span class="tagDot tagDot--cyan"></span>Пользователь: ${esc(userName)}</span>
          </div>

          <div class="metaList">
            <div><span class="mK">Part №:</span> <span class="mV mono">${esc(partNo)}</span></div>
            <div><span class="mK">OEM:</span> <span class="mV mono">${esc(oem)}</span></div>
            <div><span class="mK">Производитель:</span> <span class="mV">${esc(mfg)}</span></div>
            <div><span class="mK">Категория:</span> <span class="mV">${esc(category)}</span></div>
          </div>

          ${extrasHtml}
        </div>
      </div>

      <div class="actions">
        <button class="btn primary wide" data-take>ВЗЯТЬ ДЕТАЛЬ</button>
        <a class="btn wide" data-open href="${esc(buildItemUrl(code))}">Открыть</a>
      </div>
    </article>
  `;
}

function bindActions(){
  if(!resultsList) return;

  resultsList.querySelectorAll("[data-take]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const card = btn.closest("[data-code]");
      const code = card?.getAttribute("data-code") || "";
      alert(`Списание подключим следующим шагом.\nКод: ${code}`);
    }, { once:true });
  });
}

async function doSearch(){
  const query = String(q?.value || "").trim();
  if(!query){ showInfo("Введите запрос для поиска."); return; }

  showInfo("Поиск…");

  const uid = getUserId();
  const url = `/app/api/search?q=${encodeURIComponent(query)}&user_id=${encodeURIComponent(String(uid||0))}`;

  let data;
  try{ data = await apiGetJSON(url); }
  catch(e){ showInfo(`Ошибка поиска: ${e.message}`); return; }

  const items = Array.isArray(data?.items) ? data.items : (Array.isArray(data) ? data : []);
  setCount(items.length);

  if(!items.length){ showInfo("Ничего не найдено."); return; }

  resultsList.innerHTML = items.map(renderCard).join("");
  bindActions();
}

/* wire */
btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", (e)=>{ if(e.key==="Enter") doSearch(); });

clr?.addEventListener("click", ()=>{
  if(q) q.value = "";
  showInfo("Введите запрос для поиска.");
  try{ q?.focus?.(); }catch(_){}
});

if(resultsList) showInfo("Введите запрос для поиска.");

