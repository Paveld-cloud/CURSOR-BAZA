/* BAZA MG — Telegram-style card renderer */

const tg = window.Telegram?.WebApp;
try { tg?.expand?.(); } catch(_){}

function getUserId(){ return tg?.initDataUnsafe?.user?.id || 0; }

function esc(s){
  return String(s ?? "")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}

function pick(obj, keys, def="—"){
  for (const k of keys){
    if (obj[k] !== undefined && obj[k] !== null){
      const v = String(obj[k]).trim();
      if (v !== "") return v;
    }
  }
  return def;
}

async function api(url){
  const r = await fetch(url);
  const t = await r.text();
  try{ return JSON.parse(t); } catch(e){ throw new Error(t); }
}

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");
const list = document.getElementById("list");
const countBadge = document.querySelector(".count");

function info(msg){
  list.innerHTML = `
    <div class="card" style="margin-top:12px;">${esc(msg)}</div>
  `;
  countBadge.textContent = "0";
}

function renderCard(item){

  const code = pick(item, ["код","code"]);
  const name = pick(item, ["наименование","name"]);
  const type = pick(item, ["тип","type"]);
  const partNo = pick(item, ["парт номер","Part №","part_no"]);
  const oemPart = pick(item, ["oem парт номер","OEM парт номер"]);
  const qty = pick(item, ["количество","qty"]);
  const price = pick(item, ["цена","price"], "");
  const currency = pick(item, ["валюта","currency"], "");
  const maker = pick(item, ["изготовитель","производитель","manufacturer"]);
  const oem = pick(item, ["oem"]);

  const img = pick(item, ["image_url","image","photo"], "");

  const priceLine = price && currency ? `${price} ${currency}` :
                    price ? price : "—";

  return `
  <div class="tgCard" data-code="${esc(code)}">

    <div class="tgPhoto">
      <img src="${esc(img)}" alt="">
    </div>

    <div class="tgInfo">

      <div>🔷 <b>Код:</b> ${esc(code)}</div>
      <div>📝 <b>Наименование:</b> ${esc(name)}</div>
      <div>🔧 <b>Тип:</b> ${esc(type)}</div>
      <div>🧩 <b>Парт №:</b> ${esc(partNo)}</div>
      <div>📦 <b>OEM №:</b> ${esc(oemPart)}</div>
      <div>🔢 <b>Кол-во:</b> ${esc(qty)}</div>
      <div>💰 <b>Цена:</b> ${esc(priceLine)}</div>
      <div>🏭 <b>Изготовитель:</b> ${esc(maker)}</div>
      <div>🏷️ <b>OEM:</b> ${esc(oem)}</div>

    </div>

    <div class="actions">
      <button class="btn primary" data-take>ВЗЯТЬ</button>
      <button class="btn ghost" data-open>Открыть</button>
    </div>

  </div>
  `;
}

async function search(){
  const qv = q.value.trim();
  if(!qv){ info("Введите запрос"); return; }

  info("Поиск…");

  const url = `/app/api/search?q=${encodeURIComponent(qv)}&user_id=${getUserId()}`;

  let data;
  try{ data = await api(url); }
  catch(e){ info("Ошибка: " + e.message); return; }

  const items = data.items || [];
  countBadge.textContent = items.length;

  list.innerHTML = items.map(renderCard).join("");

  document.querySelectorAll("[data-open]").forEach(btn=>{
    btn.onclick = ()=>{
      const card = btn.closest(".tgCard");
      const code = card.dataset.code;
      window.location.href = `/item?code=${encodeURIComponent(code)}`;
    };
  });
}

btn.onclick = search;
q.onkeydown = e=>{ if(e.key==="Enter") search(); };
clr.onclick = ()=>{ q.value=""; info("Введите запрос"); };

info("Введите запрос для поиска");

