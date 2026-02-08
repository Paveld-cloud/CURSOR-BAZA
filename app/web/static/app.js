/* app.js — поиск + Telegram-style карточки + нормализация регистра */

const tg = window.Telegram?.WebApp;
try { tg.expand(); } catch (_) {}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
}

/* Полная нормализация (всё верхний регистр) */
function U(v){
  return String(v || "").trim().toUpperCase();
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
  return `
  <div class="tgCard" data-code="${U(item["код"])}">

    <div class="tgPhoto">
      <img src="${esc(item.image_url || item.image || "")}">
    </div>

    <div class="tgInfo">

      <div>🔷 <b>Код:</b> ${U(item["код"])}</div>
      <div>📝 <b>Наименование:</b> ${esc(item["наименование"])}</div>
      <div>🔧 <b>Тип:</b> ${U(item["тип"])}</div>
      <div>🧩 <b>Парт №:</b> ${U(item["парт номер"])}</div>
      <div>📦 <b>OEM №:</b> ${U(item["oem парт номер"])}</div>
      <div>🔢 <b>Кол-во:</b> ${U(item["количество"])}</div>
      <div>💰 <b>Цена:</b> ${U(item["цена"])} ${U(item["валюта"])}</div>
      <div>🏭 <b>Изготовитель:</b> ${U(item["изготовитель"])}</div>
      <div>🏷 OEM: ${U(item["oem"])}</div>

    </div>

    <div class="actions">
      <button class="btn primary wide" data-open>Открыть</button>
    </div>

  </div>
  `;
}

async function doSearch(){
  const query = q.value.trim();
  if (!query) return info("Введите запрос");

  info("Поиск…");

  const r = await fetch(`/app/api/search?q=${encodeURIComponent(query)}`);
  const data = await r.json();

  if(!data.ok){
    info("Ошибка поиска");
    return;
  }

  list.innerHTML = data.items.map(renderCard).join("");
  countBadge.textContent = data.items.length;

  document.querySelectorAll("[data-open]").forEach(btn => {
    btn.onclick = () => {
      const card = btn.closest(".tgCard");
      const code = card.dataset.code;
      window.location.href = `/item?code=${encodeURIComponent(code)}`;
    };
  });
}

btn.onclick = doSearch;
q.onkeydown = e => { if(e.key === "Enter") doSearch(); };

clr.onclick = () => {
  q.value = "";
  info("Введите запрос");
};

info("Введите запрос для поиска");
