/* item.js — детальная карточка + пересылка + полная нормализация регистра */

const tg = window.Telegram?.WebApp;
try { tg.expand(); } catch(_){}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
}

/* ВСЕГДА делаем верхний регистр */
function U(v){
    return String(v || "").trim().toUpperCase();
}

function getCode(){
  const url = new URL(window.location.href);
  return url.searchParams.get("code") || "";
}

async function loadItem(){
  const code = getCode();
  const r = await fetch(`/app/api/item?code=${encodeURIComponent(code)}`);
  const data = await r.json();

  if(!data.ok){
    tg.showAlert("Ошибка загрузки детали");
    return;
  }

  const item = data.item;

  /* Фото */
  document.getElementById("photo").src = item.image_url || item.image || "";

  /* Наименование оставляем как есть (оно может быть сложным/двухъязычным) */
  document.getElementById("title").textContent = item["наименование"] || "";

  /* ВСЕ технические параметры — строго верхний регистр */
  document.getElementById("codePill").textContent = U(item["код"]);
  document.getElementById("type").textContent = U(item["тип"]);
  document.getElementById("partNo").textContent = U(item["парт номер"]);
  document.getElementById("oemNo").textContent = U(item["oem парт номер"]);
  document.getElementById("qty").textContent = U(item["количество"]);
  document.getElementById("price").textContent = U(item["цена"]) + " " + U(item["валюта"]);
  document.getElementById("mfg").textContent = U(item["изготовитель"]);
  document.getElementById("oem").textContent = U(item["oem"]);

  /* ========== ПЕРЕСЫЛКА В TELEGRAM ========== */
  document.getElementById("shareBtn").onclick = () => {

    const text =
`🔷 КОД: ${U(item["код"])}
📝 НАИМЕНОВАНИЕ: ${item["наименование"]}
🔧 ТИП: ${U(item["тип"])}
🧩 ПАРТ №: ${U(item["парт номер"])}
📦 OEM №: ${U(item["oem парт номер"])}
🔢 КОЛ-ВО: ${U(item["количество"])}
💰 ЦЕНА: ${U(item["цена"])} ${U(item["валюта"])}
🏭 ИЗГОТОВИТЕЛЬ: ${U(item["изготовитель"])}
🏷 OEM: ${U(item["oem"])}`;

    Telegram.WebApp.openTelegramLink(
        "https://t.me/share/url?text=" + encodeURIComponent(text)
    );
  };
}

loadItem();
