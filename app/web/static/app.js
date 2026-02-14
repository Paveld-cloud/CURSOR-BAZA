/* app.js — поиск + карточки + открытие детали */

const tg = window.Telegram?.WebApp;
try { tg?.expand?.(); } catch (_) {}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
}

function U(v){ return String(v || "").trim().toUpperCase(); }

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");
const list = document.getElementById("list");
const countBadge = document.querySelector(".count");

const PLACEHOLDER_IMG =
  "data:image/svg+xml;charset=utf-8," +
  encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="800" height="450">
    <rect width="100%" height="100%" fill="#0b1829"/>
    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle"
      fill="#eaf2ff" font-size="28" font-family="Segoe UI, Arial">NO IMAGE</text>
  </svg>`);

function info(msg){
  list.innerHTML = `<div class="card" style="margin-top:12px;">${esc(msg)}</div>`;
  countBadge.textContent = "0";
}

function renderCard(item){
  const code = U(item["код"]);
  const img = item.image_url || item.image || PLACEHOLDER_IMG;

  return `
  <div class="tgCard" data-code="${esc(code)}">
    <div class="tgPhoto">
      <img src="${esc(img)}" onerror="this.src='${PLACEHOLDER_IMG}'">
    </div>

    <div class="tgInfo">
      <div>🔷 <b>Код:</b> ${esc(code)}</div>
      <div>📝 <b>Наименование:</b> ${esc(item["наименование"] || "")}</div>
      <div>🔧 <b>Тип:</b> ${esc(U(item["тип"]))}</div>
      <div>🧩 <b>Парт №:</b> ${esc(U(item["парт номер"]))}</div>
      <div>📦 <b>OEM №:</b> ${esc(U(item["oem парт номер"]))}</div>
      <div>🔢 <b>Кол-во:</b> ${esc(U(item["количество"]))}</div>
      <div>💰 <b>Цена:</b> ${esc(U(item["цена"]))} ${esc(U(item["валюта"]))}</div>
      <div>🏭 <b>Изготовитель:</b> ${esc(U(item["изготовитель"]))}</div>
      <div>🏷 OEM: ${esc(U(item["oem"]))}</div>
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

  let data;
  try {
    const r = await fetch(`/app/api/search?q=${encodeURIComponent(query)}`);
    data = await r.json();
  } catch (e) {
    return info("Ошибка сети / сервера");
  }

  if (!data?.ok){
    return info("Ошибка поиска: " + (data?.error || "unknown"));
  }

  const items = data.items || [];
  countBadge.textContent = String(items.length);

  if (!items.length){
    return info("Ничего не найдено");
  }

  list.innerHTML = items.map(renderCard).join("");

  document.querySelectorAll("[data-open]").forEach(b => {
    b.onclick = () => {
      const card = b.closest(".tgCard");
      const code = card?.dataset?.code || "";
      if (!code) return;
      window.location.href = `/item?code=${encodeURIComponent(code)}`;
    };
  });
}

btn.onclick = doSearch;
q.onkeydown = e => { if(e.key === "Enter") doSearch(); };
clr.onclick = () => { q.value = ""; info("Введите запрос"); };

info("Введите запрос для поиска");
