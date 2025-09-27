const { record, endpoint } = input.config();
const table = base.getTable('airtable_Building');
const rec = await table.selectRecordAsync(record);

const FIELD_NAMES = [
    'building_id', 'mortgagee_id', 'Address Normalized', 'bld_number', 'owner_occupied',
    'street_address', 'city', 'state', 'zip_code', 'county', 'units', 'construction_code',
    'year_built', 'stories', 'square_feet', 'desired_building_coverage',
    'fire_alarm', 'sprinkler_system',
    'roof_year_updated', 'plumbing_year_updated', 'electrical_year_updated', 'hvac_year_updated',
    'entity_id',
];

const fields = {};
for (const name of FIELD_NAMES) fields[name] = rec.getCellValue(name);


if (fields['building_id'] == null) {
    output.set('ok', false);
    output.set('error', 'Missing building_id on Airtable record');
} else {
    const resp = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fields })
    });

    const raw = await resp.text();
    let data; try { data = JSON.parse(raw); } catch { data = raw; }

    output.set('ok', resp.ok);
    output.set('syncedAt', resp.status);
    output.set('apiResult', data);
    output.set('syncedAt', new Date().toISOString());
}
