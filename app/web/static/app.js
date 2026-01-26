const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr"); // кнопка очистить
const st = document.getElementById("st");
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

function clearUI() {
  q.value = "";
  st.textContent = "";
  list.innerHTML = "";
  q.focus();
  if (tg?.HapticFeedback) tg.HapticFeedback.impactOccurred("light");
}

async function doSearch(){
  const text = (q.value||"").trim();
  if(!text){ st.textContent="Введите запрос"; return; }

  st.textContent="Ищу...";
  list.innerHTML="";

  // ВАЖНО: у тебя API теперь на /app/api/search (и есть алиас /api/search)
  const url = `/app/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;
  let res, data;

  try {
    res = await fetch(url, { cache: "no-store" });
    data = await res.json();
  } catch (e) {
    st.textContent = "Ошибка поиска. Проверьте соединение.";
    return;
  }

  if(!res.ok || !data.ok){
    st.textContent = data?.error || "Ошибка поиска";
    return;
  }

  const items = data.items || [];
  st.textContent = `Найдено: ${items.length}`;

  if(!items.length){
    list.innerHTML = `<div class="item"><div class="itemBody">Ничего не найдено</div></div>`;
    return;
  }

  for(const it of items){
    const code = (it["код"]||"").toLowerCase();
    const img = it["image_url"] || it["image"] || "";

    const html = `
      <div class="item">
        <div class="itemPhoto ${img ? "" : "noimg"}">
          ${img ? `<img class="photo" src="${esc(img)}" alt="Фото" loading="lazy" />`
                : `<div class="noPhoto">Фото не найдено</div>`}
        </div>

        <div class="itemBody">
          <div class="codeLine">
            <span>КОД: <b>${esc(it["код"]||"")}</b></span>
            <span>ОСТАТОК: <b>${esc(it["количество"]||"")}</b></span>
          </div>

          <div class="title">${esc(it["наименование"]||"")}</div>

          <div class="meta">
            <div>Тип: ${esc(it["тип"]||"")}</div>
            <div>OEM: ${esc(it["oem"]||"")}</div>
            <div>Цена: ${esc(it["цена"]||"")} ${esc(it["валюта"]||"")}</div>
          </div>

          <div class="btnRow">
            <button class="btn" data-issue="${esc(code)}">📦 Взять деталь</button>
            <button class="btn ghost" data-info="${esc(code)}">ℹ️ Описание</button>
          </div>
        </div>
      </div>
    `;
    list.insertAdjacentHTML("beforeend", html);
  }

  // ------------- Авто-адаптив фото: вертикаль/горизонталь -------------
  // Логика: если фото "очень вертикальное" -> contain, иначе cover.
  document.querySelectorAll(".photo").forEach(img => {
    img.addEventListener("load", () => {
      const w = img.naturalWidth || 1;
      const h = img.naturalHeight || 1;
      const ratio = w / h;

      // пороги можно подкрутить, но эти хорошо работают в каталоге
      if (ratio < 0.85) {
        img.classList.add("fit-contain");  // вертикальные/высокие — показываем целиком
      } else {
        img.classList.add("fit-cover");    // горизонтальные/обычные — красиво заполняем блок
      }
    }, { once: true });
  });

  // ------------- Кнопка "Описание" -------------
  document.querySelectorAll("[data-info]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const code = b.getAttribute("data-info");
      window.location.href = `/app/item?code=${encodeURIComponent(code)}`;
    });
  });

  // ------------- Кнопка "Взять деталь" -------------
  document.querySelectorAll("[data-issue]").forEach(b=>{
    b.addEventListener("click", async ()=>{
      const code = b.getAttribute("data-issue");
      const qty = prompt("Сколько списать? (пример: 1 или 2.5)");
      if(!qty) return;
      const comment = prompt("Комментарий (пример: OP-1100 авария, замена датчика)") || "";

      const payload = {
        user_id: userId(),
        name: userName(),
        code: code,
        qty: qty,
        comment: comment
      };

      let res, out;
      try {
        res = await fetch("/app/api/issue", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(payload)
        });
        out = await res.json();
      } catch (e) {
        alert("Ошибка сети при списании");
        return;
      }

      if(!res.ok || !out.ok){
        alert(out?.error || "Ошибка списания");
        return;
      }
      alert("✅ Списание записано в История");
    });
  });
}

// События
btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", e=>{ if(e.key==="Enter") doSearch(); });

// Очистить
clr?.addEventListener("click", clearUI);
