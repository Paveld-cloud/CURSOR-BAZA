// app/web/static/app.js
const tg = window.Telegram?.WebApp;
if (tg) {
  tg.expand();
  tg.ready?.();
}

const q = document.getElementById("q");
const btn = document.getElementById("btn");
const clearBtn = document.getElementById("clear");
const st = document.getElementById("st");
const list = document.getElementById("list");
const cnt = document.getElementById("cnt");

function userId() {
  return tg?.initDataUnsafe?.user?.id || 0;
}
function userName() {
  const u = tg?.initDataUnsafe?.user;
  if (!u) return "";
  const fn = (u.first_name || "").trim();
  const ln = (u.last_name || "").trim();
  return (fn + " " + ln).trim() || (u.username ? "@" + u.username : "");
}
function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function setErr(text) {
  st.classList.add("err");
  st.textContent = text || "";
}
function setOk(text) {
  st.classList.remove("err");
  st.textContent = text || "";
}

function pickPartNo(it) {
  return (
    it["парт номер"] ||
    it["парт №"] ||
    it["part"] ||
    it["part_no"] ||
    it["part number"] ||
    ""
  );
}

function pickOem(it) {
  return (
    it["oem парт номер"] ||
    it["oem"] ||
    it["oem №"] ||
    it["oem no"] ||
    it["oem number"] ||
    ""
  );
}

function renderItem(it) {
  const codeRaw = (it["код"] || "").toString();
  const code = codeRaw.trim().toLowerCase();

  const name = it["наименование"] || "";
  const type = it["тип"] || "";
  const qty = it["количество"] ?? "";
  const price = it["цена"] ?? "";
  const cur = it["валюта"] ?? "";

  // webapp.py должен отдавать image_url
  const image = it.image_url || it.image || it["image_url"] || "";

  const partNo = pickPartNo(it);
  const oem = pickOem(it);

  return `
    <div class="card">
      ${
        image
          ? `
        <div class="imgWrap">
          <img class="img" src="${esc(image)}" loading="lazy" alt="Фото"/>
        </div>
      `
          : `
        <div class="imgWrap">
          <div class="badge">без фото</div>
        </div>
      `
      }

      <div class="cardBody">
        <div class="badges">
          <div class="badge">Код: ${esc(codeRaw)}</div>
          <div class="badge">Остаток: ${esc(qty)}</div>
        </div>

        <div class="title">${esc(name)}</div>

        <div class="meta">
          Тип: ${esc(type)}<br/>
          Part №: ${esc(partNo)}<br/>
          OEM: ${esc(oem)}<br/>
          Цена: ${esc(price)} ${esc(cur)}
        </div>
      </div>

      <div class="actions">
        <button class="btn" data-issue="${esc(code)}">📦 Взять</button>
        <button class="btn ghost" data-info="${esc(code)}">ℹ️ Описание</button>
      </div>
    </div>
  `;
}

async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  let out = null;
  try {
    out = await res.json();
  } catch (e) {
    // ignore
  }
  return { res, out };
}

async function doSearch() {
  const text = (q.value || "").trim();
  if (!text) {
    setErr("Введите запрос");
    return;
  }

  setOk("Ищу...");
  list.innerHTML = "";
  cnt.textContent = "";

  const url = `/api/search?q=${encodeURIComponent(text)}&user_id=${encodeURIComponent(
    userId()
  )}`;

  let pack;
  try {
    pack = await fetchJson(url);
  } catch (e) {
    setErr("Ошибка поиска. Проверь соединение/сервер.");
    return;
  }

  const { res, out } = pack;
  if (!res.ok || !out || !out.ok) {
    setErr(out?.error || `Ошибка поиска (${res.status})`);
    return;
  }

  const items = out.items || [];
  setOk(items.length ? `Найдено: ${items.length}` : "Ничего не найдено");
  cnt.textContent = items.length ? `Найдено: ${items.length}` : "";

  if (!items.length) {
    list.innerHTML = `<div class="panel">Ничего не найдено</div>`;
    return;
  }

  // Рендер
  for (const it of items) {
    list.insertAdjacentHTML("beforeend", renderItem(it));
  }

  // Описание
  document.querySelectorAll("[data-info]").forEach((b) => {
    b.addEventListener("click", () => {
      const code = b.getAttribute("data-info");
      window.location.href = `/app/item?code=${encodeURIComponent(code)}`;
    });
  });

  // Списание
  document.querySelectorAll("[data-issue]").forEach((b) => {
    b.addEventListener("click", async () => {
      const code = b.getAttribute("data-issue");

      const qty = prompt("Сколько списать? (пример: 1 или 2.5)");
      if (!qty) return;

      const comment =
        prompt("Комментарий (пример: OP-1100 авария, замена датчика)") || "";

      const payload = {
        user_id: userId(),
        name: userName(),
        code,
        qty,
        comment,
      };

      let pack;
      try {
        pack = await fetchJson("/api/issue", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } catch (e) {
        alert("Ошибка соединения");
        return;
      }

      const { res, out } = pack;
      if (!res.ok || !out || !out.ok) {
        alert(out?.error || `Ошибка списания (${res.status})`);
        return;
      }

      alert("✅ Списание записано в История");
    });
  });
}

btn?.addEventListener("click", doSearch);
q?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doSearch();
});

clearBtn?.addEventListener("click", () => {
  q.value = "";
  list.innerHTML = "";
  cnt.textContent = "";
  setOk("");
});

// если мини-апп открыт внутри Telegram — можно подсветить тему
try {
  if (tg?.colorScheme === "dark") {
    document.documentElement.classList.add("tg-dark");
  }
} catch (e) {
  // ignore
}

