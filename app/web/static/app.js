// app/web/static/app.js
const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const st = document.getElementById("st");
const list = document.getElementById("list");
const clr = document.getElementById("clr");

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

// Универсальные геттеры: поддержка it["код"], it.code, it.row["код"]
function getRow(it){
  return (it && typeof it === "object" && it.row && typeof it.row === "object") ? it.row : it;
}
function getVal(it, keyRu, keyEn){
  const r = getRow(it) || {};
  return (r[keyRu] ?? r[keyEn] ?? it?.[keyRu] ?? it?.[keyEn] ?? "");
}
function getCode(it){
  return String(getVal(it, "код", "code") || "").trim();
}
function getName(it){
  return String(getVal(it, "наименование", "name") || "").trim();
}
function getType(it){
  return String(getVal(it, "тип", "type") || "").trim();
}
function getQty(it){
  return String(getVal(it, "количество", "qty") || "").trim();
}
function getPrice(it){
  return String(getVal(it, "цена", "price") || "").trim();
}
function getCurr(it){
  return String(getVal(it, "валюта", "currency") || "").trim();
}
function getPart(it){
  return String(getVal(it, "парт номер", "part_no") || "").trim();
}
function getOem(it){
  return String(getVal(it, "oem парт номер", "oem_part_no") || getVal(it, "oem", "oem") || "").trim();
}
function getImg(it){
  // поддержка image_url / image / row.image / row.image_url
  const r = getRow(it) || {};
  return String(r["image_url"] ?? r["image"] ?? it?.image_url ?? it?.image ?? "").trim();
}

async function doSearch(){
  const text = (q.value||"").trim();
  if(!text){ st.textContent="Введите запрос"; return; }

  st.textContent="Ищу...";
  list.innerHTML="";

  try{
    const url = `/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;
    const res = await fetch(url);
    const data = await res.json().catch(()=> ({}));

    if(!res.ok || !data.ok){
      st.textContent = `Ошибка поиска (${res.status || 0})`;
      list.innerHTML = `<div class="item">Ошибка поиска. Проверь соединение/сервер.</div>`;
      return;
    }

    const items = data.items || [];
    st.textContent = `Найдено: ${items.length}`;

    if(!items.length){
      list.innerHTML = `<div class="item">Ничего не найдено</div>`;
      return;
    }

    for(const it of items){
      const code = getCode(it);
      const codeLower = code.toLowerCase();
      const name = getName(it);
      const type = getType(it);
      const qty = getQty(it);
      const price = getPrice(it);
      const curr = getCurr(it);
      const part = getPart(it);
      const oem = getOem(it);
      const img = getImg(it);

      // ВАЖНО: отдельная строка "Код: ...." — как ты просишь
      const html = `
        <div class="item">
          <div class="itemPhoto">
            ${
              img
                ? `<img class="photo" src="${esc(img)}" alt="Фото" loading="lazy" />`
                : `<div class="noPhoto">без фото</div>`
            }
          </div>

          <div class="itemBody">
            <div class="codeLine">Код: <b>${esc(code)}</b> &nbsp; • &nbsp; Остаток: <b>${esc(qty)}</b></div>

            <div class="title">${esc(name)}</div>

            <div class="meta">
              <div><b>Тип:</b> ${esc(type)}</div>
              <div><b>Part №:</b> ${esc(part)}</div>
              <div><b>OEM:</b> ${esc(oem)}</div>
              <div><b>Цена:</b> ${esc(price)} ${esc(curr)}</div>
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
        window.location.href = `/item?code=${encodeURIComponent(code)}`;
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

        try{
          const res = await fetch("/api/issue", {
            method:"POST",
            headers:{ "Content-Type":"application/json" },
            body: JSON.stringify(payload)
          });

          const out = await res.json().catch(()=> ({}));
          if(!res.ok || !out.ok){
            alert(out.error || "Ошибка списания");
            return;
          }
          alert("✅ Списание записано в История");
        }catch(e){
          alert("Ошибка сети/сервера при списании");
        }
      });
    });

  }catch(e){
    st.textContent = "Ошибка поиска (500)";
    list.innerHTML = `<div class="item">Ошибка поиска. Проверь соединение/сервер.</div>`;
  }
}

btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", e=>{ if(e.key==="Enter") doSearch(); });

clr?.addEventListener("click", ()=>{
  q.value = "";
  st.textContent = "";
  list.innerHTML = "";
  q.focus();
});

  window.addEventListener("error", (e) => {
    setStatus("JS ошибка: " + (e?.message || "unknown"), true);
  });
})();
