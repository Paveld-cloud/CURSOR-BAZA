document.getElementById("title").textContent = item["наименование"] || "";

document.getElementById("codePill").textContent =
    normalizeValue(item["код"]);

document.getElementById("type").textContent =
    normalizeValue(item["тип"]);

document.getElementById("partNo").textContent =
    normalizeValue(item["парт номер"]);

document.getElementById("oemNo").textContent =
    normalizeValue(item["oem парт номер"]);

document.getElementById("qty").textContent =
    normalizeValue(item["количество"]);

document.getElementById("price").textContent =
    normalizeValue(item["цена"]) + " " + normalizeValue(item["валюта"]);

document.getElementById("mfg").textContent =
    normalizeValue(item["изготовитель"]);

document.getElementById("oem").textContent =
    normalizeValue(item["oem"]);

