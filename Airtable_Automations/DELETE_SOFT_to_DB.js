const { record, endpoint } = input.config();
const table = base.getTable('airtable_Building');
const rec = await table.selectRecordAsync(record);
const building_id = rec.getCellValue('building_id');
if (!building_id) { output.set('ok', false); return; }

const resp = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ building_id })
});

if (resp.ok) {
    await table.updateRecordAsync(rec, {
        'DB Synced At': new Date(),
        'Delete (send to DB)': false,
        'Status': 'Deleted'
    });
}

output.set('ok', resp.ok);