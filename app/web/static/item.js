const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

function userId() { return tg?.initDataUnsafe?.user?.id || 0; }

const img = document.getElementById("img");
const txt = document.getElementById("txt");
const codeLine = document.getElementById("codeLine");
document.getElementById("back").addEventListener("click", ()=>history.back());

const params = new URLSearchParams(location.search);
const code = (params.get("code")||"").trim();
codeLine.textContent = code ? `Код: ${code}` : "";

(async ()=>{
  if(!code){
    txt.textContent = "Нет кода детали.";
    return;
  }

  const url = `/api/item?code=${encodeURIComponent(code)}&user_id=${encodeURIComponent(userId())}`;
  const res = await fetch(url);
  const data = await res.json();

  if(!res.ok || !data.ok){
    txt.textContent = data.error || "Не найдено";
    return;
  }

  txt.textContent = data.card_text || "";

  if(data.image_url){
    img.src = data.image_url;
    img.style.display = "block";
  }
})();
