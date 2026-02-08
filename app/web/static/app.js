const tg = window.Telegram?.WebApp;
if (tg) tg.expand();

let UID = tg?.initDataUnsafe?.user?.id || 0;

// DOM
const q   = document.getElementById("q");
const btn = document.getElementById("btn");
const clr = document.getElementById("clr");
const st  = document.getElementById("st");
const list = document.getElementById("list");

// Normalize
function N(s){
    return String(s || "")
        .toUpperCase()
        .replace(/[\"\'\(\)\[\]]/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}

// SEARCH
async function search() {
    const query = N(q.value);
    if (!query) return;

    st.textContent = "Поиск...";
    list.innerHTML = "";

    const r = await fetch(`/app/search?q=${encodeURIComponent(query)}&uid=${UID}`);
    const data = await r.json();

    st.textContent = `Найдено: ${data.length}`;
    render(data);
}

// RENDER
function render(data){
    list.innerHTML = "";

    data.forEach(r => {
        const div = document.createElement("div");
        div.className = "item-card";
        div.innerHTML = `
            <div class="item-title">${r.name}</div>
            <div class="item-sub">${r.part_number}</div>
        `;
        list.appendChild(div);
    });
}

// EVENTS
btn.onclick = search;
q.onkeyup = e => { if (e.key === "Enter") search(); };
clr.onclick = () => { q.value = ""; st.textContent=""; list.innerHTML=""; };
