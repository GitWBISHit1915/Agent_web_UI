// Public FastAPI URL for ingest:
const url = "https://recloseable-dreamlessly-jamaal.ngrok-free.dev/airtable/buildings/ingest";

// ---- Inputs / record resolution ----
const cfg = input.config();
let recInput = cfg.record ?? cfg.rec;
if (Array.isArray(recInput)) recInput = recInput[0];

let foundTable = null, recObj = null;
if (typeof recInput === "string" && recInput.startsWith("rec")) {
    // resolve the record by ID (no hard-coded table name)
    for (const t of base.tables) {
        try { const r = await t.selectRecordAsync(recInput); if (r) { foundTable = t; recObj = r; break; } } catch { }
    }
}

// ---- Helpers ----
function htmlToFields(s) {
    const out = {};
    if (typeof s !== "string") return out;
    s = s.replace(/\r?\n/g, ""); // collapse whitespace
    const re = /<b>(.*?)<\/b><br\/>(.*?)(?:<br\/>){2,}/g;
    let m;
    while ((m = re.exec(s)) !== null) {
        const key = m[1].trim();
        const valStr = m[2].trim();
        let parsed = valStr;
        if (/^-?\d+$/.test(valStr)) parsed = parseInt(valStr, 10);
        else if (/^-?\d+\.\d+$/.test(valStr)) parsed = parseFloat(valStr);
        else if (valStr === "true" || valStr === "false") parsed = (valStr === "true");
        out[key] = parsed;
    }
    return out;
}

// Build fields payload from whichever shape we received
let fields = {};
if (recObj && foundTable) {
    // load all fields from the live record
    for (const f of foundTable.fields) {
        fields[f.name] = recObj.getCellValue(f.name);
    }
} else if (recInput && typeof recInput === "object" && recInput.fields && typeof recInput.fields === "object") {
    fields = recInput.fields; // legacy object shape
} else if (typeof recInput === "string") {
    fields = htmlToFields(recInput); // HTML blob
}

// ---- GUARD: if building_id already present, no-op (and auto-uncheck) ----
if (fields?.building_id != null && String(fields.building_id).trim() !== "") {
    if (recObj && foundTable) {
        // revert the add checkbox so it doesn't stay checked
        await foundTable.updateRecordAsync(recObj, { 'Add Building (send to DB)': false });
    }
    const addr_norm_guard = fields['Address Normalized'] ?? fields.address_normalized ?? null;
    output.set("building_id", Number(fields.building_id));
    output.set("address_normalized", addr_norm_guard);
    output.set("skipped", "already has building_id");
    return;
}

// ---- Call ingest API (only when no building_id) ----
const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
});

const text = await res.text();
let building_id = null, addr_norm = fields['Address Normalized'] ?? fields.address_normalized ?? null;
try {
    const parsed = JSON.parse(text);
    building_id = parsed.building_id ?? null;
    addr_norm = parsed.address_normalized ?? addr_norm ?? null;
} catch (_) {
    // keep defaults if response isn't JSON
}

// ---- Write back to Airtable (inside script), then expose outputs ----
if (recObj && foundTable) {
    const updatePayload = {
        'Add Building (send to DB)': false,            // uncheck after send
        'DB Synced At': new Date(),
    };
    if (building_id != null) updatePayload['building_id'] = Number(building_id);
    if (addr_norm != null) updatePayload['Address Normalized'] = addr_norm;

    await foundTable.updateRecordAsync(recObj, updatePayload);
}

output.set("building_id", building_id != null ? Number(building_id) : null);
output.set("address_normalized", addr_norm ?? null);
output.set("ok", res.ok);
