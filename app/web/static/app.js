// ===============================
//   Telegram Mini App Init
// ===============================
const tg = window.Telegram?.WebApp || null;

let USER_ID = 0;
let USER_NAME = "";
let initOK = false;

if (tg) {
    tg.expand(); // во весь экран

    const u = tg.initDataUnsafe?.user;
    if (u && u.id) {
        USER_ID = u.id;
        USER_NAME = `${u.first_name || ""} ${u.last_name || ""}`.trim();
        initOK = true;
    } else {
        console.warn("⚠ Telegram user_id не получен");
    }
} else {
    console.warn("⚠ Telegram.WebApp API не найдено");
}


// ===============================
//   DOM элементы
// ===============================
const q   = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");
const st  = document.getElementById("st");
const cnt = document.getElementById("cnt");
const list = document.getElementById("list");


// ===============================
//  Нормализация текста (как в боте)
// ===============================
function N(s) {
    return String(s || "")
        .toUpperCase()
        .replace(/[\"\'\(\)\[\]]/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}


// ===============================
//  Поиск
// ===============================
async function doSearch() {
    const query = N(q.value);
    if (!query) {
        list.innerHTML = "";
        st.textContent = "";
        return;
    }

    st.textContent = "⏳ Поиск...";
    list.innerHTML = "";

    try {
        const resp = await fetch(`/app/search?q=${encodeURIComponent(query)}&uid=${USER_ID}`);
        if (!resp.ok) throw new Error("Network error");

        const data = await resp.json();
        showResults(data);
    } catch (err) {
        console.error(err);
        st.textContent = "Ошибка поиска";
    }
}


// ===============================
//  Показ результатов
// ===============================
function showResults(rows) {
    list.innerHTML = "";
    if (!rows || !rows.length) {
        st.textContent = "Ничего не найдено";
        return;
    }

    st.textContent = `Найдено: ${rows.length}`;

    for (const r of rows) {
        const card = document.createElement("div");
        card.className = "item-card";

        const img = r.image_url || "/static/noimg.png";

        card.innerHTML = `
            <img src="${img}" class="item-img" />
            <div class="item-title">${r.name || ""}</div>
            <div class="item-sub">${r.part_number || ""}</div>

            <button class="btn-take" data-pn="${r.part_number}" data-name="${r.name}">
                ВЗЯТЬ ДЕТАЛЬ
            </button>
        `;

        list.appendChild(card);
    }

    // навешиваем обработчики на кнопки
    document.querySelectorAll(".btn-take").forEach(btn => {
        btn.addEventListener("click", (e) => {
            const pn = e.target.dataset.pn;
            const name = e.target.dataset.name;
            openTakeForm(pn, name);
        });
    });
}


// ===============================
//  Очистка
// ===============================
clr.addEventListener("click", () => {
    q.value = "";
    list.innerHTML = "";
    st.textContent = "";
});

btn.addEventListener("click", doSearch);
q.addEventListener("keyup", (e) => {
    if (e.key === "Enter") doSearch();
});


// ===============================
//  Форма «Взять деталь»
// ===============================
function openTakeForm(pn, name) {
    const qty = prompt(`Сколько взять?\n${name} (${pn})`);
    if (!qty || isNaN(qty) || qty <= 0) return;

    const com = prompt("Комментарий (необязательно):") || "";

    if (!confirm(`Подтвердить списание ${qty} шт?\n${name} (${pn})`)) return;

    sendTake(pn, name, qty, com);
}


// ===============================
//  Отправка списания
// ===============================
async function sendTake(pn, name, qty, comment) {
    try {
        const body = {
            uid: USER_ID,
            user_name: USER_NAME,
            pn,
            name,
            qty,
            comment
        };

        const resp = await fetch("/app/take", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(body)
        });

        const data = await resp.json();

        if (data?.ok) {
            tg?.showPopup?.({
                title: "Готово",
                message: "Списание записано",
                buttons: [{id: "ok", type: "default", text: "OK"}]
            });
        } else {
            throw new Error("Bad response");
        }

    } catch (err) {
        console.error(err);
        alert("Ошибка записи списания");
    }
}
