/* item.js — детальная карточка + пересылка + нормализация регистра */

const tg = window.Telegram?.WebApp;
try { tg.expand(); } catch(_){}

function esc(s){
  return String(s ?? "").replace(/[&<>]/g, c => (
    {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]
  ));
}

/* Универсальная нормализация */
function normalizeValue(v) {
    if (!v) return "";
    return String(v).trim().toUpperCase();
}

/* Получение кода детали из URL */
function getCode(){
  const url = new URL(window.location.href);
  return url.searchParams.get("code") || "";
}

/* Загрузка детали с backend */
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

  /* Заголовок остаётся как есть — названия у тебя бывают в смешанном стиле */
  document.getElementById("title").textContent = item["наименование"] || "";

  /* Технические поля — ВСЕГДА В ВЕРХНЕМ РЕГИСТРЕ */
  document.getElementById("codePill").textContent = normalizeValue(item["код"]);
  document.getElementById("type").textContent = normalizeValue(item["тип"]);
  document.getElementById("partNo").textContent = normalizeValue(item["парт номер"]);
  document.getElementById("oemNo").textContent = normalizeValue(item["oem парт номер"]);
  document.getElementById("qty").textContent = normalizeValue(item["количество"]);
  document.getElementById("price").textContent =
      normalizeValue(item["цена"]) + " " + normalizeValue(item["валюта"]);
  document.getElementById("mfg").textContent = normalizeValue(item["изготовитель"]);
  document.getElementById("oem").textContent = normalizeValue(item["oem"]);

  /* ===== КНОПКА «ПЕРЕСЛАТЬ» ===== */
  document.getElementById("shareBtn").onclick = () => {

    /* Формируем Telegram-сообщение в нужном стиле */
    const text =
`🔷 КОД: ${normalizeValue(item["код"])}
📝 НАИМЕНОВАНИЕ: ${item["наименование"]}
🔧 ТИП: ${normalizeValue(item["тип"])}
🧩 ПАРТ №: ${normalizeValue(item["парт номер"])}
📦 OEM №: ${normalizeValue(item["oem парт номер"])}
🔢 КОЛ-ВО: ${normalizeValue(item["количество"])}
💰 ЦЕНА: ${normalizeValue(item["цена"])} ${normalizeValue(item["валюта"])}
🏭 ИЗГОТОВИТЕЛЬ: ${normalizeValue(item["изготовитель"])}
🏷 OEM: ${normalizeValue(item["oem"])}`;

    /* Открываем окно пересылки в Telegram */
    Telegram.WebApp.openTelegramLink(
      "https://t.me/share/url?text=" + encodeURIComponent(text)
    );
  };
}

loadItem();
