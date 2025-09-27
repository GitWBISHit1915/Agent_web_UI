// Replace with your public FastAPI URL:
const url = "https://recloseable-dreamlessly-jamaal.ngrok-free.dev/airtable/buildings/ingest"

const cfg = input.config();
let rec = cfg.rec;
if (Array.isArray(rec)) rec = rec[0];

function htmlToFields(s) {
    const out = {};
    if (typeof s !== "string") return out;

    // collapse whitespace
    s = s.replace(/\r?\n/g, "");

    // matches: <b>key</b><br/>value<br/><br/>  (repeated)
    const re = /<b>(.*?)<\/b><br\/>(.*?)(?:<br\/>){2,}/g;
    let match;
    while ((match = re.exec(s)) !== null) {
        const key = match[1].trim();
        let valStr = match[2].trim();

        // light coercion
        let parsed = valStr;
        if (/^-?\d+$/.test(valStr)) parsed = parseInt(valStr, 10);
        else if (/^-?\d+\.\d+$/.test(valStr)) parsed = parseFloat(valStr);
        else if (valStr === "true" || valStr === "false") parsed = (valStr === "true");

        out[key] = parsed;
    }
    return out;
}

let fields = {};
if (rec && typeof rec === "object" && rec.fields && typeof rec.fields === "object") {
    fields = rec.fields;            // normal record shape
} else if (typeof rec === "string") {
    fields = htmlToFields(rec);     // HTML blob shape
} else {
    fields = {};                    // fallback
}

console.log("FIELDS KEYS:", Object.keys(fields));

const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
});

const text = await res.text();
console.log(res.status, text);

let building_id = null, addr_norm = null
try {
    const parsed = JSON.parse(text);
    building_id = parsed.building_id ?? null;
    addr_norm = parsed.address_normalized ?? fields.address_normalized ?? null;
} catch (e) {
    console.log("Could not parse response", e);
    addr_norm = fields.address_normalized ?? null;
}

output.set("building_id", building_id != null ? Number(building_id) : null);
output.set("address_normalized", addr_norm);

