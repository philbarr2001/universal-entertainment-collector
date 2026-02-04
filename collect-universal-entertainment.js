// ============================================================
// Universal Orlando Entertainment Schedule Collector
// Railway Node.js Script
// 
// Fetches show calendar data from Universal's Tridion CMS
// endpoints, detects changes, and upserts to Supabase.
// ============================================================

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://wkvezozqmbnvlxgdmbys.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY; // Set in Railway env vars

// --- Show Configuration ---
const TRACKED_SHOWS = [
  {
    show_id: 'universal-studios-cinesational',
    show_name: 'CineSational: A Symphonic Spectacular',
    park_name: 'Universal Studios Florida',
    show_type: 'NIGHTTIME_SHOW',
    cms_url: 'https://www.universalorlando.com/webdata/k2/en/us/things-to-do/shows/cinesational-symphonic-spectacular/index.html'
  },
  {
    show_id: 'universal-studios-mega-movie-parade',
    show_name: 'Universal Mega Movie Parade',
    park_name: 'Universal Studios Florida',
    show_type: 'PARADE',
    cms_url: 'https://www.universalorlando.com/webdata/k2/en/us/things-to-do/shows/universal-mega-movie-parade/index.html'
  },
  {
  id: 'usf-mardi-gras-parade',
  name: 'Universal Mardi Gras Parade',
  park: 'Universal Studios Florida',
  type: 'parade',
  url: 'https://www.universalorlando.com/webdata/k2/en/us/things-to-do/events/mardi-gras/parade/index.html'
},
  {
    show_id: 'islands-of-adventure-hogwarts-lights',
    show_name: 'The Nighttime Lights at Hogwarts Castle',
    park_name: "Universal's Islands of Adventure",
    show_type: 'PROJECTION_SHOW',
    cms_url: 'https://www.universalorlando.com/webdata/k2/en/us/things-to-do/entertainment/the-nighttime-lights-at-hogwarts-castle/index.html'
  }
];

// ============================================================
// CMS Parsing
// ============================================================

/**
 * Fetch the CMS page model JSON for a show
 */
async function fetchCMSData(url) {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; MouseCounselors/1.0)',
      'Accept': 'text/html,application/json'
    }
  });

  if (!response.ok) {
    throw new Error(`CMS fetch failed: ${response.status} ${response.statusText}`);
  }

  const text = await response.text();
  return JSON.parse(text);
}

/**
 * Recursively search the CMS JSON for calendarConfig data.
 * The calendar is embedded in a ComponentPresentation whose
 * Component.Fields contains a calendarConfig field.
 */
function findCalendarConfig(obj, depth = 0) {
  if (depth > 25 || !obj || typeof obj !== 'object') return null;

  // Direct hit — the calendarConfig field itself
  if (obj.calendarConfig) {
    return obj.calendarConfig;
  }

  // Check Fields → calendarConfig pattern
  if (obj.Fields && obj.Fields.calendarConfig) {
    return obj.Fields.calendarConfig;
  }

  // Recurse into arrays
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const result = findCalendarConfig(item, depth + 1);
      if (result) return result;
    }
    return null;
  }

  // Recurse into object values
  for (const key of Object.keys(obj)) {
    // Skip large irrelevant branches to stay fast
    if (['MetadataFields', 'Categories', 'Multimedia'].includes(key)) continue;
    const result = findCalendarConfig(obj[key], depth + 1);
    if (result) return result;
  }

  return null;
}

/**
 * Check if a show is marked "Temporarily Closed" in its utility section.
 * This handles shows like Hogwarts Lights that have no calendar.
 */
function checkTemporarilyClosed(cmsData) {
  try {
    const presentations = cmsData.ComponentPresentations || [];
    for (const cp of presentations) {
      const features = cp.Component?.Fields?.featureList?.LinkedComponentValues || [];
      for (const feature of features) {
        const desc = feature.Fields?.description?.Values?.[0] || '';
        if (desc.toLowerCase().includes('temporarily closed')) {
          return true;
        }
      }
    }
  } catch (e) {
    // Ignore parse errors
  }
  return false;
}

/**
 * Parse calendarConfig into an array of { date, show_time, status } objects.
 * 
 * calendarConfig structure (from CMS):
 *   EmbeddedValues: [
 *     {
 *       eventDates: { DateTimeValues: ["2026-01-05T00:00:00", ...] },
 *       blockData: {
 *         EmbeddedValues: [{
 *           eyebrow: { Values: ["8:30 PM"] },
 *           style: { Values: ["Active Style"] }  // or "Disabled Style" for tentative
 *         }]
 *       }
 *     },
 *     ...
 *   ]
 */
/**
 * Recursively search an object for a key and return its value.
 * Used as a fallback when the exact path isn't known.
 */
function findValueByKey(obj, targetKey, depth = 0) {
  if (depth > 10 || !obj || typeof obj !== 'object') return null;
  
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const result = findValueByKey(item, targetKey, depth + 1);
      if (result !== null) return result;
    }
    return null;
  }
  
  for (const key of Object.keys(obj)) {
    if (key === targetKey) return obj[key];
    const result = findValueByKey(obj[key], targetKey, depth + 1);
    if (result !== null) return result;
  }
  return null;
}

/**
 * Extract the eyebrow (show time) and style from a blockData object.
 * Tries multiple Tridion CMS patterns since the nesting varies.
 */
function extractBlockInfo(blockData) {
  let showTime = 'Unknown';
  let isTentative = false;

  if (!blockData) return { showTime, isTentative };

  // Try multiple Tridion CMS patterns for the nested block
  const blockEntries = 
    blockData.EmbeddedValues ||
    blockData.LinkedComponentValues ||
    blockData.Values ||
    [];

  if (Array.isArray(blockEntries) && blockEntries.length > 0) {
    const block = blockEntries[0];

    // The eyebrow might be directly on the block, or nested under Fields
    const eyebrowObj = block.eyebrow || block.Fields?.eyebrow;
    if (eyebrowObj) {
      showTime = eyebrowObj.Values?.[0] || eyebrowObj.Value || 'Unknown';
    }

    // Same for style
    const styleObj = block.style || block.Fields?.style;
    if (styleObj) {
      const styleVal = styleObj.Values?.[0] || styleObj.Value || '';
      isTentative = styleVal.toLowerCase().includes('disabled');
    }
  }

  // Fallback: recursively search blockData for eyebrow
  if (showTime === 'Unknown') {
    const eyebrowFound = findValueByKey(blockData, 'eyebrow');
    if (eyebrowFound) {
      showTime = eyebrowFound.Values?.[0] || eyebrowFound.Value || 
                 (typeof eyebrowFound === 'string' ? eyebrowFound : 'Unknown');
    }
  }

  // Fallback: recursively search for style
  if (!isTentative) {
    const styleFound = findValueByKey(blockData, 'style');
    if (styleFound) {
      const styleVal = styleFound.Values?.[0] || styleFound.Value || 
                       (typeof styleFound === 'string' ? styleFound : '');
      isTentative = styleVal.toLowerCase().includes('disabled');
    }
  }

  return { showTime, isTentative };
}

function parseCalendarConfig(calendarConfig) {
  const schedules = [];
  let debugged = false;

  const entries = calendarConfig.EmbeddedValues || calendarConfig.LinkedComponentValues || calendarConfig.Values || [];
  if (!Array.isArray(entries)) {
    console.log(`    ⚠ calendarConfig has no iterable entries. Keys: ${Object.keys(calendarConfig).join(', ')}`);
    return schedules;
  }

  for (const entry of entries) {
    // Extract dates
    const dateTimeValues = entry.eventDates?.DateTimeValues || [];
    if (dateTimeValues.length === 0) continue;

    // Debug: log the blockData structure for the first entry
    if (!debugged && entry.blockData) {
      const bdKeys = Object.keys(entry.blockData);
      console.log(`    blockData keys: ${bdKeys.join(', ')}`);
      for (const k of bdKeys) {
        const val = entry.blockData[k];
        if (Array.isArray(val) && val.length > 0) {
          const firstKeys = typeof val[0] === 'object' ? Object.keys(val[0]).join(', ') : typeof val[0];
          console.log(`    blockData.${k}[0] keys: ${firstKeys}`);
          // If it has Fields, show those too
          if (val[0].Fields) {
            console.log(`    blockData.${k}[0].Fields keys: ${Object.keys(val[0].Fields).join(', ')}`);
          }
        }
      }
      debugged = true;
    } else if (!debugged) {
      console.log(`    ⚠ First entry has no blockData. Entry keys: ${Object.keys(entry).join(', ')}`);
      debugged = true;
    }

    // Extract show time and tentative status
    const { showTime, isTentative } = extractBlockInfo(entry.blockData);

    // Create a schedule record for each date
    for (const dt of dateTimeValues) {
      const dateStr = dt.split('T')[0]; // "2026-01-05"
      schedules.push({
        schedule_date: dateStr,
        show_time: showTime,
        status: isTentative ? 'TENTATIVE' : 'SCHEDULED',
        is_available: !isTentative
      });
    }
  }

  return schedules;
}

// ============================================================
// Supabase Operations
// ============================================================

async function supabaseRequest(path, method, body, extraHeaders = {}) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    ...extraHeaders
  };

  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);

  const response = await fetch(url, options);
  
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Supabase ${method} ${path}: ${response.status} - ${errorText}`);
  }

  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return response.json();
  }
  return null;
}

/**
 * Fetch existing schedule records for a show (future dates only)
 */
async function fetchExistingSchedules(showId) {
  const today = new Date().toISOString().split('T')[0];
  const path = `universal_entertainment_schedules?show_id=eq.${showId}&schedule_date=gte.${today}&select=*`;
  return supabaseRequest(path, 'GET');
}

/**
 * Upsert a batch of schedule records
 */
async function upsertSchedules(records) {
  if (records.length === 0) return;

  // Supabase REST upsert — POST with merge-duplicates on UNIQUE(show_id, schedule_date)
  return supabaseRequest(
    'universal_entertainment_schedules?on_conflict=show_id,schedule_date',
    'POST',
    records,
    { 'Prefer': 'resolution=merge-duplicates,return=minimal' }
  );
}

/**
 * Log detected changes for the notification system
 */
async function logChanges(changes) {
  if (changes.length === 0) return;
  return supabaseRequest(
    'universal_schedule_changes',
    'POST',
    changes,
    { 'Prefer': 'return=minimal' }
  );
}

/**
 * Upsert the show status summary row
 */
async function upsertShowStatus(statusRecord) {
  return supabaseRequest(
    'universal_show_status?on_conflict=show_id',
    'POST',
    statusRecord,
    { 'Prefer': 'resolution=merge-duplicates,return=minimal' }
  );
}

// ============================================================
// Change Detection
// ============================================================

function detectChanges(showConfig, newSchedules, existingSchedules) {
  const changes = [];
  const existingMap = new Map();

  for (const existing of existingSchedules) {
    existingMap.set(existing.schedule_date, existing);
  }

  for (const newRec of newSchedules) {
    const existing = existingMap.get(newRec.schedule_date);

    if (!existing) {
      // Brand new date added
      changes.push({
        change_type: 'ENTERTAINMENT',
        entity_id: showConfig.show_id,
        entity_name: showConfig.show_name,
        change_date: newRec.schedule_date,
        change_description: `New show date added: ${showConfig.show_name} on ${newRec.schedule_date} at ${newRec.show_time}`,
        old_value: null,
        new_value: { show_time: newRec.show_time, status: newRec.status },
        severity: 'LOW'
      });
    } else {
      // Check for time change
      if (existing.show_time !== newRec.show_time) {
        changes.push({
          change_type: 'ENTERTAINMENT',
          entity_id: showConfig.show_id,
          entity_name: showConfig.show_name,
          change_date: newRec.schedule_date,
          change_description: `Show time changed for ${showConfig.show_name} on ${newRec.schedule_date}: ${existing.show_time} → ${newRec.show_time}`,
          old_value: { show_time: existing.show_time },
          new_value: { show_time: newRec.show_time },
          severity: 'MEDIUM'
        });
      }

      // Check for status change
      if (existing.status !== newRec.status) {
        changes.push({
          change_type: 'ENTERTAINMENT',
          entity_id: showConfig.show_id,
          entity_name: showConfig.show_name,
          change_date: newRec.schedule_date,
          change_description: `Status changed for ${showConfig.show_name} on ${newRec.schedule_date}: ${existing.status} → ${newRec.status}`,
          old_value: { status: existing.status },
          new_value: { status: newRec.status },
          severity: 'MEDIUM'
        });
      }
    }
  }

  // Check for removed dates (existed in DB but not in new CMS data)
  const newDateSet = new Set(newSchedules.map(s => s.schedule_date));
  for (const existing of existingSchedules) {
    if (!newDateSet.has(existing.schedule_date)) {
      changes.push({
        change_type: 'ENTERTAINMENT',
        entity_id: showConfig.show_id,
        entity_name: showConfig.show_name,
        change_date: existing.schedule_date,
        change_description: `Show date removed: ${showConfig.show_name} on ${existing.schedule_date} (was ${existing.show_time})`,
        old_value: { show_time: existing.show_time, status: existing.status },
        new_value: null,
        severity: 'HIGH'
      });
    }
  }

  return changes;
}

// ============================================================
// Main Collection Loop
// ============================================================

async function processShow(showConfig) {
  console.log(`\n--- Processing: ${showConfig.show_name} ---`);

  // 1. Fetch CMS data
  let cmsData;
  try {
    cmsData = await fetchCMSData(showConfig.cms_url);
    console.log(`  ✓ CMS data fetched`);
  } catch (error) {
    console.error(`  ✗ CMS fetch failed: ${error.message}`);
    return { show: showConfig.show_id, error: error.message, schedules: 0, changes: 0 };
  }

  // 2. Check for Temporarily Closed (no calendar data)
  const isClosed = checkTemporarilyClosed(cmsData);

  // 3. Find and parse calendar config
  const calendarConfig = findCalendarConfig(cmsData);
  let newSchedules = [];

  if (calendarConfig) {
    newSchedules = parseCalendarConfig(calendarConfig);
    console.log(`  ✓ Parsed ${newSchedules.length} scheduled dates`);
    if (newSchedules.length > 0) {
      console.log(`    First: ${newSchedules[0].schedule_date} (${newSchedules[0].show_time})`);
      console.log(`    Last:  ${newSchedules[newSchedules.length - 1].schedule_date} (${newSchedules[newSchedules.length - 1].show_time})`);
    }
  } else if (isClosed) {
    console.log(`  ⚠ Show is Temporarily Closed — no calendar data`);
  } else {
    // Debug: list all top-level Field keys to help diagnose
    const cpCount = cmsData.ComponentPresentations?.length || 0;
    console.log(`  ⚠ No calendarConfig found (${cpCount} ComponentPresentations)`);
    for (const cp of (cmsData.ComponentPresentations || [])) {
      const fieldKeys = Object.keys(cp.Component?.Fields || {});
      if (fieldKeys.length > 0) {
        console.log(`    CP "${cp.Component?.Id}": ${fieldKeys.join(', ')}`);
      }
    }
  }

  // 4. Determine current status
  let currentStatus;
  if (isClosed) {
    currentStatus = 'TEMPORARILY_CLOSED';
  } else if (newSchedules.length > 0) {
    currentStatus = 'ACTIVE';
  } else {
    currentStatus = 'UNKNOWN';
  }

  // 5. Fetch existing records for change detection
  let existingSchedules = [];
  try {
    existingSchedules = await fetchExistingSchedules(showConfig.show_id);
    console.log(`  ✓ Found ${existingSchedules.length} existing records in Supabase`);
  } catch (error) {
    console.error(`  ⚠ Could not fetch existing records: ${error.message}`);
  }

  // 6. Detect changes
  const changes = detectChanges(showConfig, newSchedules, existingSchedules);
  if (changes.length > 0) {
    console.log(`  ⚡ ${changes.length} changes detected`);
    changes.forEach(c => console.log(`     - ${c.change_description}`));
  } else {
    console.log(`  ✓ No changes detected`);
  }

  // 7. Build upsert records
  const now = new Date().toISOString();
  const records = newSchedules.map(s => ({
    show_id: showConfig.show_id,
    show_name: showConfig.show_name,
    park_name: showConfig.park_name,
    schedule_date: s.schedule_date,
    show_time: s.show_time,
    show_type: showConfig.show_type,
    status: s.status,
    is_available: s.is_available,
    source_url: showConfig.cms_url,
    collected_at: now,
    last_modified: now
  }));

  // 8. Upsert schedules to Supabase
  if (records.length > 0) {
    try {
      // Batch in chunks of 100 to avoid payload limits
      for (let i = 0; i < records.length; i += 100) {
        const batch = records.slice(i, i + 100);
        await upsertSchedules(batch);
      }
      console.log(`  ✓ Upserted ${records.length} schedule records`);
    } catch (error) {
      console.error(`  ✗ Upsert failed: ${error.message}`);
    }
  }

  // 9. Log changes
  if (changes.length > 0) {
    try {
      await logChanges(changes);
      console.log(`  ✓ Logged ${changes.length} changes`);
    } catch (error) {
      console.error(`  ✗ Change logging failed: ${error.message}`);
    }
  }

  // 10. Update show status summary
  const futureDates = newSchedules
    .map(s => s.schedule_date)
    .filter(d => d >= new Date().toISOString().split('T')[0])
    .sort();

  try {
    await upsertShowStatus({
      show_id: showConfig.show_id,
      show_name: showConfig.show_name,
      park_name: showConfig.park_name,
      show_type: showConfig.show_type,
      current_status: currentStatus,
      next_scheduled_date: futureDates[0] || null,
      last_scheduled_date: futureDates[futureDates.length - 1] || null,
      total_scheduled_dates: newSchedules.length,
      cms_url: showConfig.cms_url,
      last_checked_at: now,
      last_updated_at: changes.length > 0 ? now : undefined
    });
    console.log(`  ✓ Show status updated: ${currentStatus}`);
  } catch (error) {
    console.error(`  ✗ Status update failed: ${error.message}`);
  }

  return {
    show: showConfig.show_id,
    status: currentStatus,
    schedules: records.length,
    changes: changes.length
  };
}

async function main() {
  console.log('===========================================');
  console.log('Universal Orlando Entertainment Collector');
  console.log(`Started: ${new Date().toISOString()}`);
  console.log('===========================================');

  if (!SUPABASE_KEY) {
    console.error('ERROR: SUPABASE_KEY environment variable is not set');
    process.exit(1);
  }

  const results = [];

  for (const show of TRACKED_SHOWS) {
    try {
      const result = await processShow(show);
      results.push(result);
    } catch (error) {
      console.error(`\nFATAL error processing ${show.show_id}: ${error.message}`);
      results.push({ show: show.show_id, error: error.message, schedules: 0, changes: 0 });
    }
  }

  // Summary
  console.log('\n===========================================');
  console.log('Collection Summary');
  console.log('===========================================');
  const totalSchedules = results.reduce((sum, r) => sum + (r.schedules || 0), 0);
  const totalChanges = results.reduce((sum, r) => sum + (r.changes || 0), 0);
  const errors = results.filter(r => r.error);

  results.forEach(r => {
    const icon = r.error ? '✗' : '✓';
    console.log(`  ${icon} ${r.show}: ${r.schedules} dates, ${r.changes} changes${r.error ? ` (ERROR: ${r.error})` : ''}`);
  });

  console.log(`\nTotal: ${totalSchedules} schedules, ${totalChanges} changes, ${errors.length} errors`);
  console.log(`Finished: ${new Date().toISOString()}`);
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
