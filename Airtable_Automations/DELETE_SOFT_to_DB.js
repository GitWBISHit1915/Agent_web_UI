// Inputs: record (ID), deleteUrl, restoreUrl
const { record, deleteUrl, restoreUrl } = input.config();

// Locate record (no hard-coded table name)
let tbl = null, rec = null;
for (const t of base.tables) {
    try { const r = await t.selectRecordAsync(record); if (r) { tbl = t; rec = r; break; } } catch { }
}
if (!rec) { output.set('ok', false); output.set('error', `Record ${record} not found`); return; }

// Read current state
const box = !!rec.getCellValue('Delete (send to DB)');         // checkbox true/false
const status = rec.getCellValueAsString('Status') || '';        // e.g., 'Deleted' or 'Active'
const bldId = rec.getCellValue('building_id');
if (!bldId) { output.set('ok', false); output.set('error', 'missing building_id'); return; }

// Helper to stamp Airtable consistently
async function stamp(fields) {
    await tbl.updateRecordAsync(rec, Object.assign({ 'DB Synced At': new Date() }, fields));
}

// Decide action:
// - If box is checked and NOT already Deleted => DELETE
// - If box is unchecked and IS Deleted => RESTORE
// - Otherwise, do nothing (idempotent)
if (box && status !== 'Deleted') {
    const resp = await fetch(deleteUrl, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ building_id: bldId })
    });
    output.set('delete_status', resp.status);
    if (resp.ok) {
        await stamp({ 'Delete (send to DB)': true, 'Status': 'Deleted' });
        output.set('ok', true);
    } else {
        // revert checkbox if API failed so UI reflects truth
        await stamp({ 'Delete (send to DB)': false });
        output.set('ok', false);
    }
} else if (!box && status === 'Deleted') {
    const resp = await fetch(restoreUrl, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ building_id: bldId })
    });
    output.set('restore_status', resp.status);
    if (resp.ok) {
        await stamp({ 'Delete (send to DB)': false, 'Status': null });
        output.set('ok', true);
    } else {
        // revert checkbox if API failed
        await stamp({ 'Delete (send to DB)': true });
        output.set('ok', false);
    }
} else {
    // No-op: user re-clicked same state or other fields changed
    output.set('ok', true);
    output.set('noop', true);
}
