// Input: changesUrl = https://<host>/airtable/buildings/changes
const { changesUrl } = input.config();

// Tables
const buildings = base.getTable('airtable_Building');     // exact table name
const control = base.getTable('airtable_syncControl');  // holds Last Cursor (1 record)

// Optional: Sync Lock support (unchecked means go)
let syncLock = false;
try {
    const ctrlQ0 = await control.selectRecordsAsync({ fields: ['Sync Lock'] });
    const ctrl0 = ctrlQ0.records[0];
    syncLock = !!ctrl0?.getCellValue('Sync Lock');
    if (syncLock) {
        output.set('ok', true);
        output.set('skipped', 'Sync Lock checked');
        return;
    }
} catch { /* field may not exist yet; ignore */ }

// Load cursor (or epoch)
const ctrlQ = await control.selectRecordsAsync({ fields: ['Last Cursor'] });
const ctrl = ctrlQ.records[0] ?? null;
const sinceISO =
    ctrl?.getCellValue('Last Cursor')?.toISOString?.() ?? '1970-01-01T00:00:00Z';

// Fetch changes
const resp = await fetch(`${changesUrl}?since=${encodeURIComponent(sinceISO)}`);
if (!resp.ok) {
    output.set('ok', false);
    output.set('status', resp.status);
    output.set('error', 'changes endpoint failed');
    return;
}
const data = await resp.json();
const nowISO = data.now;
const upserts = Array.isArray(data.upserts) ? data.upserts : [];
const deletes = Array.isArray(data.deletes) ? data.deletes : [];

// Build a quick index of existing Airtable rows by building_id
const abQ = await buildings.selectRecordsAsync({ fields: ['building_id'] });
const byId = new Map();
for (const r of abQ.records) {
    const idVal = r.getCellValue('building_id');
    const key = idVal == null ? '' : String(idVal);
    if (key) byId.set(key, r);
}

// Valid target field set (for safety)
const validTargets = new Set(buildings.fields.map(f => f.name));

// Collect warnings about missing Airtable fields (targets)
const skippedTargets = new Set();

// ---- Mapping: server row -> Airtable fields ----
// Use your exact mapping requirement here:
function mapRowToAirtableFields(row) {
    // Helper: set only if value is not undefined, and target exists
    const out = {};
    const setIf = (target, value) => {
        if (value === undefined) return;
        if (!validTargets.has(target)) { skippedTargets.add(target); return; }
        out[target] = value;
    };

    // Your mapping (server -> Airtable). Keep 'DB Synced At' always.
    setIf('building_id', row.building_id);
    setIf('Address Normalized', row['Address Normalized']);        // server sends this exact key
    setIf('Bld#', row['bld_number']);
    setIf('Owner Occupied', row['owner_occupied']);
    setIf('Street Address', row['street_address']);
    setIf('City', row['city']);
    setIf('State', row['state']);
    setIf('Zip', row['zip_code']);
    setIf('County', row['county']);
    setIf('Units', row['units']);
    setIf('construction_code', row['construction_code']);
    setIf('Year Built', row['year_built']);
    setIf('Stories', row['stories']);
    setIf('Square Feet', row['square_feet']);
    setIf('Desired Building Coverage', row['desired_building_coverage']);
    setIf('Fire Alarm', row['fire_alarm']);
    setIf('Sprinkler System', row['sprinkler_system']);
    setIf('roof_year_updated', row['roof_year_updated']);
    setIf('plumbing_year_updated', row['plumbing_year_updated']);
    setIf('electrical_year_updated', row['electrical_year_updated']);
    setIf('hvac_year_updated', row['hvac_year_updated']);
    setIf('entity_id', row['entity_id']);

    // Always stamp synced time (only if field exists)
    if (validTargets.has('DB Synced At')) out['DB Synced At'] = new Date();

    return out;
}

// Chunk helper per Airtable limits
const chunk = (arr, n) =>
    Array.from({ length: Math.ceil(arr.length / n) }, (_, i) => arr.slice(i * n, (i + 1) * n));

// Prepare upserts
const toUpdate = [];
const toCreate = [];
for (const row of upserts) {
    const idStr = String(row.building_id ?? '');
    if (!idStr) continue;

    const fields = mapRowToAirtableFields(row);
    const existing = byId.get(idStr);
    if (existing) toUpdate.push({ id: existing.id, fields });
    else toCreate.push({ fields });
}

// Apply in batches (50 max per call)
let updatesApplied = 0, createsApplied = 0, deletesApplied = 0;

for (const batch of chunk(toUpdate, 50)) {
    if (batch.length) {
        await buildings.updateRecordsAsync(batch);
        updatesApplied += batch.length;
    }
}
for (const batch of chunk(toCreate, 50)) {
    if (batch.length) {
        await buildings.createRecordsAsync(batch);
        createsApplied += batch.length;
    }
}

// Apply deletes in Airtable: mark Status=Deleted, keep delete box checked
const delOps = [];
for (const row of deletes) {
    const idStr = String(row.building_id ?? '');
    if (!idStr) continue;
    const r = byId.get(idStr);
    if (!r) continue;

    const fields = {};
    if (validTargets.has('Status')) fields['Status'] = 'Deleted';
    if (validTargets.has('Delete (send to DB)')) fields['Delete (send to DB)'] = true;
    if (validTargets.has('DB Synced At')) fields['DB Synced At'] = new Date();
    if (Object.keys(fields).length) delOps.push({ id: r.id, fields });
}
for (const batch of chunk(delOps, 50)) {
    if (batch.length) {
        await buildings.updateRecordsAsync(batch);
        deletesApplied += batch.length;
    }
}

// Advance the cursor
if (ctrl) {
    await control.updateRecordAsync(ctrl, { 'Last Cursor': new Date(nowISO) });
} else {
    await control.createRecordAsync({ 'Last Cursor': new Date(nowISO) });
}

// Outputs
const skippedList = Array.from(skippedTargets);
if (skippedList.length) output.set('skipped_fields', skippedList);
output.set('ok', true);
output.set('updates_applied', updatesApplied);
output.set('creates_applied', createsApplied);
output.set('deletes_applied', deletesApplied);
