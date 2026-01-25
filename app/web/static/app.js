const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

const q = document.getElementById("q");
const btn = document.getElementById("btn");
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

function esc(s){return String(s??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")}

async function doSearch(){
  const text = (q.value||"").trim();
  if(!text){ st.textContent="Введите запрос"; return; }

  st.textContent="Ищу...";
  list.innerHTML="";

  const url = `/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(userId())}`;
  const res = await fetch(url);
  const data = await res.json();

  if(!res.ok || !data.ok){
    st.textContent = data.error || "Ошибка поиска";
    return;
  }

  const items = data.items || [];
  st.textContent = `Найдено: ${items.length}`;

  if(!items.length){
    list.innerHTML = `<div class="item">Ничего не найдено</div>`;
    return;
  }

  for(const it of items){
    const code = (it["код"]||"").toLowerCase();
    const html = `
      <div class="item">
        <div class="itemHead">
          <div>
            <div class="code">🔢 ${esc(it["код"]||"")}</div>
            <div class="name">📄 ${esc(it["наименование"]||"")}</div>
            <div class="meta">Тип: ${esc(it["тип"]||"")} • Кол-во: ${esc(it["количество"]||"")} • Цена: ${esc(it["цена"]||"")} ${esc(it["валюта"]||"")}</div>
          </div>
        </div>

        <div class="btnRow">
          <button class="btn" data-issue="${esc(code)}">📦 Взять деталь</button>
          <button class="btn ghost" data-info="${esc(code)}">ℹ️ Описание</button>
        </div>
      </div>
    `;
    list.insertAdjacentHTML("beforeend", html);
  }

  document.querySelectorAll("[data-info]").forEach(b=>{
    b.addEventListener("click", ()=>{
      const code = b.getAttribute("data-info");
      window.location.href = `/item?code=${encodeURIComponent(code)}`;
    });
  });

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

      const res = await fetch("/api/issue", {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      const out = await res.json();
      if(!res.ok || !out.ok){
        alert(out.error || "Ошибка списания");
        return;
      }
      alert("✅ Списание записано в История");
    });
  });
}

btn.addEventListener("click", doSearch);
q.addEventListener("keydown", e=>{ if(e.key==="Enter") doSearch(); });
