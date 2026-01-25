// app/web/static/app.js  (ID под твой index.html: q, btn, clear, st, list, cnt)

(function () {
  const $ = (id) => document.getElementById(id);

  const tg = window.Telegram?.WebApp;
  if (tg) {
    try { tg.expand(); tg.ready(); } catch {}
  }

  function setStatus(text, isErr = false) {
    const st = $("st");
    if (!st) return;
    st.textContent = text || "";
    st.classList.toggle("err", !!isErr);
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function renderEmpty(msg = "") {
    const cnt = $("cnt");
    const list = $("list");
    if (cnt) cnt.textContent = "";
    if (list) list.innerHTML = "";
    if (msg) setStatus(msg, false);
  }

  function renderCards(items) {
    const cnt = $("cnt");
    const list = $("list");
    if (cnt) cnt.textContent = `Найдено: ${items.length}`;
    if (!list) return;

    list.innerHTML = "";

    items.forEach((item) => {
      const code = escapeHtml(item.code || "");
      const name = escapeHtml(item.name || "");
      const type = escapeHtml(item.type || "");
      const part = escapeHtml(item.part || "");
      const oem = escapeHtml(item.oem || "");
      const qty = escapeHtml(item.qty || "");
      const price = escapeHtml(item.price || "");
      const currency = escapeHtml(item.currency || "");
      const image = item.image || "";

      const card = document.createElement("div");
      card.className = "card";

      if (image) {
        const img = document.createElement("img");
        img.className = "img";
        img.src = image;
        img.alt = "Фото";
        img.onerror = () => {
          img.remove();
          const no = document.createElement("div");
          no.className = "no-photo";
          no.textContent = "без фото";
          card.prepend(no);
        };
        card.appendChild(img);
      } else {
        const no = document.createElement("div");
        no.className = "no-photo";
        no.textContent = "без фото";
        card.appendChild(no);
      }

      const body = document.createElement("div");
      body.className = "card-body";
      body.innerHTML = `
        <div class="pill-row">
          <div class="pill">Код <b>${code}</b></div>
          <div class="pill green">Остаток <b>${qty}</b></div>
        </div>

        <div class="title">${name}</div>

        ${type ? `<div class="row"><span class="k">Тип:</span> <span class="v">${type}</span></div>` : ""}
        ${part ? `<div class="row"><span class="k">Part №:</span> <span class="v">${part}</span></div>` : ""}
        ${oem ? `<div class="row"><span class="k">OEM:</span> <span class="v">${oem}</span></div>` : ""}
        ${(price || currency) ? `<div class="row"><span class="k">Цена:</span> <span class="v">${price} ${currency}</span></div>` : ""}

        <div class="actions">
          <button class="btn primary" data-action="issue" data-code="${code}">📦 Взять</button>
          <button class="btn ghost" data-action="open" data-code="${code}">ℹ️ Описание</button>
        </div>
      `;
      card.appendChild(body);

      list.appendChild(card);
    });
  }

  async function doSearch() {
    const qEl = $("q");
    const q = (qEl?.value || "").trim();

    if (!q) {
      renderEmpty("Введите запрос");
      return;
    }

    setStatus("Поиск…");

    const userId = window.Telegram?.WebApp?.initDataUnsafe?.user?.id || "";
    const url = `/api/search?q=${encodeURIComponent(q)}${userId ? `&user_id=${encodeURIComponent(userId)}` : ""}`;

    let res;
    try {
      res = await fetch(url, { method: "GET", cache: "no-store" });
    } catch (e) {
      setStatus("Ошибка сети (fetch)", true);
      return;
    }

    if (!res.ok) {
      setStatus(`Ошибка поиска (${res.status})`, true);
      return;
    }

    let data;
    try {
      data = await res.json();
    } catch (e) {
      setStatus("Ошибка ответа (JSON)", true);
      return;
    }

    const items = Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : []);

    if (!items.length) {
      renderEmpty("Ничего не найдено");
      return;
    }

    setStatus("");
    renderCards(items);
  }

  function clearAll() {
    const qEl = $("q");
    if (qEl) qEl.value = "";
    setStatus("");
    renderEmpty();
  }

  function bind() {
    const btn = $("btn");
    const clear = $("clear");
    const qEl = $("q");

    // Маркер, что JS реально загрузился
    setStatus("JS OK");

    if (!btn || !clear || !qEl) {
      setStatus("JS: не найдены элементы (q/btn/clear)", true);
      return;
    }

    btn.addEventListener("click", doSearch);
    clear.addEventListener("click", clearAll);

    // Делегирование кликов по кнопкам карточек
    const list = $("list");
    if (list) {
      list.addEventListener("click", (e) => {
        const el = e.target;
        if (!el || !el.dataset) return;
        const code = (el.dataset.code || "").trim();
        const action = (el.dataset.action || "").trim();
        if (!code || !action) return;

        // Ведём на страницу /item (там и описание, и списание)
        if (action === "open" || action === "issue") {
          window.location.href = `/item?code=${encodeURIComponent(code)}`;
        }
      });
    }

    qEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter") doSearch();
    });

    // Уберём "JS OK" через 1 секунду
    setTimeout(() => {
      if ($("st")?.textContent === "JS OK") setStatus("");
    }, 1000);
  }

  // Если скрипт подключился раньше DOM — ждём
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }

  // Глобальный перехват ошибок
  window.addEventListener("error", (e) => {
    setStatus("JS ошибка: " + (e?.message || "unknown"), true);
  });
})();

  window.addEventListener("error", (e) => {
    setStatus("JS ошибка: " + (e?.message || "unknown"), true);
  });
})();

