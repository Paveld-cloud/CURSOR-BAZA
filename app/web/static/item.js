/* item.js — детальная карточка + пересылка + нормализация регистра */

const tg = window.Telegram?.WebApp;
try { tg.expand(); } catch(_){}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
}

function normalizeValue(v) {
    if (!v) return "";
    return String(v).trim().toUpperCase();
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

  document.getElementById("codePill").textContent = normalizeValue(item["код"]);
  document.getElementById("type").textContent = normalizeValue(item["тип"]);
  document.getElementById("partNo").textContent = normalizeValue(item["парт номер"]);
  document.getElementById("oemNo").textContent = normalizeValue(item["oem парт номер"]);
  document.getElementById("qty").textContent = normalizeValue(item["количество"]);
  document.getElementById("price").textContent =
      normalizeValue(item["цена"]) + " " + normalizeValue(item["валюта"]);
  document.getElementById("mfg").textContent = normalizeValue(item["изготовитель"]);
  document.getElementById("oem").textContent = normalizeValue(item["oem"]);

  // ===== Пересылка карточки =====
  document.getElementById("shareBtn").onclick = () => {

    const text =
`🔷 Код: ${normalizeValue(item["код"])}
📝 Наименование: ${item["наименование"]}
🔧 Тип: ${normalizeValue(item["тип"])}
🧩 Парт №: ${normalizeValue(item["парт номер"])}
📦 OEM №: ${normalizeValue(item["oem парт номер"])}
🔢 Кол-во: ${normalizeValue(item["количество"])}
💰 Цена: ${normalizeValue(item["цена"])} ${normalizeValue(item["валюта"])}
🏭 Изготовитель: ${normalizeValue(item["изготовитель"])}
🏷 OEM: ${normalizeValue(item["oem"])}`;

    Telegram.WebApp.openTelegramLink(
      "https://t.me/share/url?text=" + encodeURIComponent(text)
    );
  };
}

loadItem();
