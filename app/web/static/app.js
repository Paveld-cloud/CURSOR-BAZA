// app/web/static/app.js
const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

// Если Mini App открыта на /app, то API чаще всего тоже на /app/api/*
// Если открыта в корне, то API на /api/*
const API_PREFIX = window.location.pathname.startsWith("/app") ? "/app" : "";

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const st = document.getElementById("st");
const list = document.getElementById("list");
const clr = document.getElementById("clr"); // может отсутствовать — это ок

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

// Универсальные геттеры (поддержка it.row и прямых полей)
function getRow(it){
  return (it && typeof it === "object" && it.row && typeof it.row === "object") ? it.row : it;
}
function getVal(it, keyRu, keyEn){
  const r = getRow(it) || {};
  return (r[keyRu] ?? r[keyEn] ?? it?.[keyRu] ?? it?.[keyEn] ?? "");
}
function getCode(it){ return String(getVal(it, "код", "code") || "").trim(); }
function getName(it){ return String(getVal(it, "наименование", "name") || "").trim(); }
function getType(it){ return String(getVal(it, "тип", "type") || "").trim(); }
function getQty(it){ return String(getVal(it, "количество", "qty") || "").trim(); }
function getPrice(it){ return String(getVal(it, "цена", "price") || "").trim(); }
function getCurr(it){ return String(getVal(it, "валюта", "currency") || "").trim(); }
function getPart(it){ return String(getVal(it, "парт номер", "part_no") || "").trim(); }
function getOem(it){
  return String(
    getVal(it, "oem парт номер", "oem_part_no") ||
    getVal(it, "oem", "oem") ||
    ""
  ).trim();
}
function getImg(it){
  const r = getRow(it) || {};
  return String(r["image_url"] ?? r["image"] ?? it?.image_url ?? it?.image ?? "").trim();
}

async function doSearch(){
  const text = (q?.value || "").trim();
  if(!text){
    if (st) st.textContent = "Введите запрос";
    return;
  }

  if (st) st.textContent = "Ищу...";
  if (list) list.innerHTML = "";

  const url = `${API_PREFIX}/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;

  let res, data;
  try{
    res = await fetch(url, { method: "GET" });
    data = await res.json();
  }catch(e){
    if (st) st.textContent = "Ошибка сети/сервера";
    if (list) list.innerHTML = `<div class="item">Ошибка поиска. Проверь соединение/сервер.</div>`;
    return;
  }

  if(!res.ok || !data?.ok){
    if (st) st.textContent = `Ошибка поиска (${res.status})`;
    if (list) list.innerHTML = `<div class="item">Ошибка поиска. Проверь сервер/API путь.</div>`;
    return;
  }

  const items = data.items || [];
  if (st) st.textContent = `Найдено: ${items.length}`;

  if(!items.length){
    if (list) list.innerHTML = `<div class="item">Ничего не найдено</div>`;
    return;
  }

  for(const it of items){
    const code = getCode(it);
    const codeLower = code.toLowerCase();

    const html = `
      <div class="item">
        <div class="itemPhoto">
          ${
            getImg(it)
              ? `<img class="photo" src="${esc(getImg(it))}" alt="Фото" loading="lazy" />`
              : `<div class="noPhoto">без фото</div>`
          }
        </div>

        <div class="itemBody">
          <div class="codeLine">Код: <b>${esc(code)}</b> &nbsp; • &nbsp; Остаток: <b>${esc(getQty(it))}</b></div>
          <div class="title">${esc(getName(it))}</div>

          <div class="meta">
            <div><b>Тип:</b> ${esc(getType(it))}</div>
            <div><b>Part №:</b> ${esc(getPart(it))}</div>
            <div><b>OEM:</b> ${esc(getOem(it))}</div>
            <div><b>Цена:</b> ${esc(getPrice(it))} ${esc(getCurr(it))}</div>
          </div>

          <div class="btnRow">
            <button class="btn" data-issue="${esc(codeLower)}">📦 Взять</button>
            <button class="btn ghost" data-info="${esc(codeLower)}">ℹ️ Описание</button>
          </div>
        </div>
      </div>
    `;
    list.insertAdjacentHTML("beforeend", html);
  }

  // Описание
  document.querySelectorAll("[data-info]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const code = b.getAttribute("data-info");
      window.location.href = `${API_PREFIX}/item?code=${encodeURIComponent(code)}`;
    });
  });

  // Списание
  document.querySelectorAll("[data-issue]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      const code = b.getAttribute("data-issue");

      const qty = prompt("Сколько списать? (пример: 1 или 2.5)");
      if(!qty) return;

      const comment = prompt("Комментарий (пример: OP-1100, замена датчика)") || "";

      const payload = {
        user_id: userId(),
        name: userName(),
        code: code,
        qty: qty,
        comment: comment
      };

      let res, out;
      try{
        res = await fetch(`${API_PREFIX}/api/issue`, {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(payload)
        });
        out = await res.json();
      }catch(e){
        alert("Ошибка сети/сервера при списании");
        return;
      }

      if(!res.ok || !out?.ok){
        alert(out?.error || "Ошибка списания");
        return;
      }
      alert("✅ Списание записано в История");
    });
  });
}

btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", e=>{ if(e.key==="Enter") doSearch(); });

clr?.addEventListener("click", ()=>{
  q.value = "";
  if (st) st.textContent = "";
  if (list) list.innerHTML = "";
  q.focus();
});
