/* item.js — детальная карточка + пересылка + ВЗЯТЬ (списание в История) */

const tg = window.Telegram?.WebApp;
try { tg?.expand?.(); } catch(_){}

function U(v){
  return String(v || "").trim().toUpperCase();
}

function getUser(){
  const u = tg?.initDataUnsafe?.user;
  return {
    user_id: u?.id || 0,
    name: (`${u?.first_name || ""} ${u?.last_name || ""}`.trim()) || (u?.username ? "@"+u.username : "")
  };
}

function getCode(){
  const url = new URL(window.location.href);
  return url.searchParams.get("code") || "";
}

const PLACEHOLDER_IMG =
  "data:image/svg+xml;charset=utf-8," +
  encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="800" height="450">
    <rect width="100%" height="100%" fill="#0b1829"/>
    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle"
      fill="#eaf2ff" font-size="28" font-family="Segoe UI, Arial">NO IMAGE</text>
  </svg>`);

async function loadItem(){
  const code = getCode();
  if (!code) {
    tg?.showAlert?.("Код не указан");
    return;
  }

  const r = await fetch(`/app/api/item?code=${encodeURIComponent(code)}`);
  const data = await r.json();

  if(!data.ok){
    tg?.showAlert?.("Ошибка загрузки детали");
    return;
  }

  const item = data.item || {};

  // Фото
  const photoEl = document.getElementById("photo");
  photoEl.src = item.image_url || item.image || PLACEHOLDER_IMG;
  photoEl.onerror = () => { photoEl.src = PLACEHOLDER_IMG; };

  // Текстовые поля
  document.getElementById("title").textContent = item["наименование"] || "";
  document.getElementById("codePill").textContent = U(item["код"]);
  document.getElementById("type").textContent = U(item["тип"]);
  document.getElementById("partNo").textContent = U(item["парт номер"]);
  document.getElementById("oemNo").textContent = U(item["oem парт номер"]);
  document.getElementById("qty").textContent = U(item["количество"]);
  document.getElementById("price").textContent = `${U(item["цена"])} ${U(item["валюта"])}`.trim();
  document.getElementById("mfg").textContent = U(item["изготовитель"]);
  document.getElementById("oem").textContent = U(item["oem"]);

  // Переслать
  document.getElementById("shareBtn").onclick = () => {
    const text =
`🔷 КОД: ${U(item["код"])}
📝 НАИМЕНОВАНИЕ: ${item["наименование"] || ""}
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

  // ВЗЯТЬ (списание)
  document.getElementById("takeBtn").onclick = async () => {
    const { user_id, name } = getUser();

    const qty = prompt(`Сколько взять?\nКод: ${U(item["код"])}`);
    if (!qty || !String(qty).trim()) return;

    const comment = prompt("Комментарий (необязательно):") || "";

    const ok = confirm(`Подтвердить списание?\nКод: ${U(item["код"])}\nКол-во: ${qty}`);
    if (!ok) return;

    try {
      const resp = await fetch("/app/api/issue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id,
          name,
          code: item["код"],
          qty: String(qty).trim(),
          comment: String(comment).trim()
        })
      });

      const out = await resp.json();
      if (out?.ok) {
        tg?.showAlert?.("✅ Списание записано в История");
      } else {
        tg?.showAlert?.("❌ Ошибка: " + (out?.error || "unknown"));
      }
    } catch (e) {
      tg?.showAlert?.("❌ Ошибка сети / сервера");
    }
  };
}

loadItem();
