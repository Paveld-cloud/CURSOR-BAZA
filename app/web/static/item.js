/* item.js — детальная карточка + пересылка в Telegram */

const tg = window.Telegram?.WebApp;
try { tg.expand(); } catch(_){}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
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

  document.getElementById("photo").src = item.image_url || item.image || "";
  document.getElementById("title").textContent = item["наименование"] || "";
  document.getElementById("codePill").textContent = item["код"] || "";
  document.getElementById("type").textContent = item["тип"] || "";
  document.getElementById("partNo").textContent = item["парт номер"] || "";
  document.getElementById("oemNo").textContent = item["oem парт номер"] || "";
  document.getElementById("qty").textContent = item["количество"] || "";
  document.getElementById("price").textContent =
    (item["цена"] || "") + " " + (item["валюта"] || "");
  document.getElementById("mfg").textContent = item["изготовитель"] || "";
  document.getElementById("oem").textContent = item["oem"] || "";

  // ========== Пересылка карточки ==========
  document.getElementById("shareBtn").onclick = () => {

    const text =
`🔷 Код: ${item["код"]}
📝 Наименование: ${item["наименование"]}
🔧 Тип: ${item["тип"]}
🧩 Парт №: ${item["парт номер"]}
📦 OEM №: ${item["oem парт номер"]}
🔢 Кол-во: ${item["количество"]}
💰 Цена: ${item["цена"]} ${item["валюта"]}
🏭 Изготовитель: ${item["изготовитель"]}
🏷 OEM: ${item["oem"]}`;

    Telegram.WebApp.openTelegramLink(
      "https://t.me/share/url?text=" + encodeURIComponent(text)
    );
  };
}

loadItem();


