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
          <button class="btn primary" data-action="open" data-code="${code}">📦 Взять</button>
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

    // ВАЖНО: чтобы ты видел, что клик реально сработал
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

    let dataJson;
    try {
      dataJson = await res.json();
    } catch (e) {
      setStatus("Ошибка ответа (JSON)", true);
      return;
    }

    const items = Array.isArray(dataJson) ? dataJson : (Array.isArray(dataJson?.items) ? dataJson.items : []);

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
    const list = $("list");

    // Показываем 2 секунды, чтобы точно заметил
    setStatus("JS OK");
    setTimeout(() => {
      if ($("st")?.textContent === "JS OK") setStatus("");
    }, 2000);

    if (!btn || !clear || !qEl) {
      setStatus("JS: не найдены элементы (q/btn/clear)", true);
      return;
    }

    // ДВОЙНОЕ привязывание: addEventListener + onclick
    btn.addEventListener("click", doSearch);
    btn.onclick = doSearch;

    clear.addEventListener("click", clearAll);
    clear.onclick = clearAll;

    qEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter") doSearch();
    });

    // клики по карточкам (переход на /item)
    if (list) {
      list.addEventListener("click", (e) => {
        const t = e.target;
        if (!t || !t.dataset) return;
        const code = (t.dataset.code || "").trim();
        const act = (t.dataset.action || "").trim();
        if (!code || !act) return;
        window.location.href = `/item?code=${encodeURIComponent(code)}`;
      });
    }

    // экспортируем наружу (fallback для onclick из HTML)
    window.MG_DO_SEARCH = doSearch;
    window.MG_CLEAR = clearAll;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }

  window.addEventListener("error", (e) => {
    setStatus("JS ошибка: " + (e?.message || "unknown"), true);
  });
})();
