/*
 * "Storage" console section: where the lake keeps its data files and BLOBs,
 * the (read-only) Postgres catalog, and snapshot retention. Data-file and BLOB
 * settings are saved to the gateway's storage config file and applied by a
 * restart (src/console/storage-admin.js). Loaded after app.js and uses its
 * helpers (h, api, icon, modal, toast, withBusy, field, pageHeader, settingRows).
 */
'use strict';

(function () {
    const SOURCE_LABEL = { console: 'saved in the console', env: 'from docker/.env', default: 'default' };

    registerView('storage', async (ctx) => {
        const info = await api('GET', '/storage');
        const isAdmin = ctx.isAdmin;
        const form = structuredClone(info.form);
        const initial = structuredClone(info.form);
        const locked = !info.lake.canMove;   // data location fixed while tables have files

        // ---------------------------------------------------------------- inputs
        const input = (value, attrs = {}) => h('input', { type: 'text', value: value ?? '', readOnly: !isAdmin, ...attrs });
        const localPath = input(form.localPath || info.envForm.localPath || '/app/data_files', { placeholder: '/app/data_files' });
        const s3Path = input(form.s3.path, { placeholder: 's3://bucket/prefix/' });
        const endpoint = input(form.s3.endpoint, { placeholder: 'empty for AWS S3, e.g. minio:9000' });
        const region = input(form.s3.region, { placeholder: 'us-east-1' });
        const accessKey = input(form.s3.accessKey, { placeholder: 'empty: AWS credential chain (IAM role, ~/.aws)', autocomplete: 'off' });
        const secretKey = input('', { type: 'password', autocomplete: 'new-password', placeholder: form.s3.hasSecretKey ? '•••••••• (unchanged)' : 'secret access key' });
        const sessionToken = input('', { type: 'password', autocomplete: 'off', placeholder: form.s3.hasSessionToken ? '•••••••• (unchanged)' : 'only for temporary credentials' });
        const urlStyle = h('select', { disabled: !isAdmin },
            [['', 'Automatic (path with an endpoint, vhost for AWS)'], ['path', 'Path style (MinIO, most self-hosted)'], ['vhost', 'Virtual-hosted (AWS)']]
                .map(([v, l]) => h('option', { value: v, selected: v === form.s3.urlStyle }, l)));
        const useSsl = h('input', { type: 'checkbox', checked: form.s3.useSsl, disabled: !isAdmin });
        const inlining = input(String(form.inliningLimit), { type: 'number', min: 0, max: 1000000 });
        const blobPath = input(form.blobs.path, { placeholder: '/app/blobs' });
        const blobConfirm = h('input', { type: 'checkbox' });

        // Data-file backend tiles; S3 connection fields move between the S3
        // panel and the "credentials for imports" panel of the local backend.
        const s3Fields = h('div', {},
            h('div', { class: 'form-grid' }, field('Endpoint', endpoint, 'host:port of an S3-compatible server. Leave empty for AWS S3.'), field('Region', region)),
            h('div', { class: 'form-grid' }, field('Access key', accessKey), field('Secret key', secretKey,
                form.s3.hasSecretKey ? 'Leave empty to keep the saved secret.' : null)),
            h('div', { class: 'form-grid' }, field('URL style', urlStyle), h('div', { class: 'field' }, h('label', {}, 'Transport'), h('label', { class: 'checkbox' }, useSsl, 'Use TLS (https)'))),
            h('details', { class: 'advanced' }, h('summary', {}, 'Temporary credentials'),
                field('Session token', sessionToken, 'Only for temporary (STS) credentials.')));
        const s3Panel = h('div', { class: 'backend-panel' },
            field('Bucket path', s3Path, 'Bucket and prefix the lake writes to, e.g. s3://my-bucket/lake/. Use an empty prefix: this lake\'s files must not mix with others.'));
        const importCreds = h('details', { class: 'advanced' }, h('summary', {}, 'S3 credentials for bulk imports from s3:// (optional)'));
        const localPanel = h('div', { class: 'backend-panel' },
            field('Directory', localPath, 'In Docker, use a mounted directory: /app/data_files is persisted to docker/data/ducklake.'),
            persistenceWarning(info.persistence.data),
            importCreds);

        const tile = (name, value, title, desc, iconName) => {
            const radio = h('input', { type: 'radio', name, value, checked: (name === 'backend' ? form.backend : form.blobs.mode) === value });
            const el = h('label', { class: 'choice' }, radio, h('span', { class: 'choice-icon' }, icon(iconName)),
                h('span', {}, h('strong', {}, title), h('span', { class: 'muted small' }, desc)));
            return { el, radio };
        };
        const tLocal = tile('backend', 'local', 'Local disk', 'A directory on this host or a mounted volume', 'database');
        const tS3 = tile('backend', 's3', 'S3-compatible', 'AWS S3, MinIO, Cloudflare R2, ...', 'upload');
        const tInline = tile('blobs', 'inline', 'Inside tables', 'BLOB bytes are stored in the Parquet files', 'table');
        const tFs = tile('blobs', 'filesystem', 'Local directory', 'Files on disk; tables store their paths', 'database');
        for (const t of [tLocal, tS3, tInline, tFs]) t.radio.disabled = !isAdmin;
        if (locked) { tLocal.radio.disabled = tS3.radio.disabled = true; localPath.readOnly = true; s3Path.readOnly = true; }

        const blobDirField = h('div', {}, field('BLOB directory', blobPath, 'In Docker, /app/blobs is persisted to docker/data/blobs.'), persistenceWarning(info.persistence.blobs));
        const blobConfirmRow = h('label', { class: 'checkbox callout' }, blobConfirm,
            'I understand: BLOBs stored so far are not moved or converted. After changing the format or directory they can no longer be read through the API.');

        const sync = () => {
            const s3 = tS3.radio.checked;
            localPanel.hidden = s3;
            s3Panel.hidden = !s3;
            (s3 ? s3Panel : importCreds).appendChild(s3Fields);
            blobDirField.hidden = !tFs.radio.checked;
            const blobChanged = (tFs.radio.checked ? 'filesystem' : 'inline') !== initial.blobs.mode ||
                (tFs.radio.checked && blobPath.value.trim() !== initial.blobs.path);
            blobConfirmRow.hidden = !blobChanged;
        };
        for (const el of [tLocal.radio, tS3.radio, tInline.radio, tFs.radio]) el.addEventListener('change', sync);
        blobPath.addEventListener('input', sync);
        sync();

        const read = () => ({
            backend: tS3.radio.checked ? 's3' : 'local',
            localPath: localPath.value,
            s3: {
                path: s3Path.value, endpoint: endpoint.value, region: region.value, accessKey: accessKey.value,
                secretKey: secretKey.value, sessionToken: sessionToken.value, urlStyle: urlStyle.value, useSsl: useSsl.checked,
            },
            inliningLimit: Number(inlining.value),
            blobs: { mode: tFs.radio.checked ? 'filesystem' : 'inline', path: blobPath.value },
            confirmBlobChange: blobConfirm.checked,
        });

        const fillFrom = (f) => {
            tLocal.radio.checked = f.backend === 'local';
            tS3.radio.checked = f.backend === 's3';
            localPath.value = f.localPath || localPath.value;
            s3Path.value = f.s3.path; endpoint.value = f.s3.endpoint; region.value = f.s3.region; accessKey.value = f.s3.accessKey;
            urlStyle.value = f.s3.urlStyle; useSsl.checked = f.s3.useSsl; inlining.value = String(f.inliningLimit);
            tInline.radio.checked = f.blobs.mode === 'inline'; tFs.radio.checked = f.blobs.mode === 'filesystem'; blobPath.value = f.blobs.path;
            secretKey.value = ''; sessionToken.value = '';
            sync();
        };

        // --------------------------------------------------------------- actions
        const results = h('div', { class: 'test-results' });
        const showResults = (r, title) => {
            results.replaceChildren(h('div', { class: `callout ${r.ok ? 'info' : 'error'}` },
                h('strong', {}, title || (r.ok ? 'Connection test passed' : 'Connection test failed')),
                h('ul', { class: 'plain' }, (r.results || []).map(x => h('li', {},
                    h('span', { class: `badge ${x.ok ? 'ok' : 'off'}` }, x.ok ? 'ok' : 'failed'), ' ', h('strong', {}, `${x.label}: `), x.detail,
                    x.warning ? h('div', { class: 'warn-text' }, x.warning) : null)))));
        };
        const showError = (message) => results.replaceChildren(h('div', { class: 'callout error' }, message));

        const testBtn = h('button', { class: 'btn', type: 'button' }, 'Test connection');
        testBtn.addEventListener('click', async () => {
            testBtn.disabled = true;
            results.replaceChildren(h('div', { class: 'muted' }, 'Testing…'));
            try { showResults(await api('POST', '/storage/test', read())); }
            catch (err) { showError(err.message); }
            finally { testBtn.disabled = false; }
        });

        const saveBtn = h('button', { class: 'btn primary', type: 'button' }, info.autoRestart ? 'Save & restart' : 'Save');
        saveBtn.addEventListener('click', async () => {
            const next = read();
            const changes = describeChanges(initial, next);
            if (!changes.length) return showError('Nothing changed.');
            const moving = changes.some(c => c.move);
            const ok = await modal({
                title: info.autoRestart ? 'Save and restart the gateway?' : 'Save storage settings?',
                body: h('div', { class: 'row-gap' },
                    h('ul', { class: 'plain' }, changes.map(c => h('li', {}, h('strong', {}, `${c.label}: `), c.text))),
                    moving ? h('div', { class: 'callout' }, 'The lake moves to the new location when the gateway starts. Files at the old location are left in place; remove them yourself once you have checked the lake.') : null,
                    h('p', { class: 'muted' }, info.autoRestart
                        ? 'The settings are tested first. The gateway then restarts (the API and console are unavailable for a few seconds). If it cannot start with the new settings it goes back to the current ones and shows the error here.'
                        : 'The settings are tested first. Restart the gateway yourself to apply them.')),
                actions: [{ label: 'Cancel', value: false }, { label: info.autoRestart ? 'Save & restart' : 'Save', value: true, primary: true }],
            });
            if (!ok) return;
            saveBtn.disabled = testBtn.disabled = true;
            results.replaceChildren(h('div', { class: 'muted' }, 'Testing and saving…'));
            try {
                const res = await api('PUT', '/storage', next);
                if (res.restarting) await waitForRestart(results);
                else {
                    results.replaceChildren(h('div', { class: 'callout info' }, 'Saved. Restart the gateway to apply the new storage settings.'));
                    toast('Storage settings saved; restart required', 'success');
                }
            } catch (err) {
                showError(err.message);
            } finally {
                saveBtn.disabled = testBtn.disabled = false;
            }
        });
        const resetBtn = h('button', { class: 'btn', type: 'button', title: 'Fill the form with the docker/.env values (then save to apply)' }, 'Use docker/.env values');
        resetBtn.addEventListener('click', () => { fillFrom(info.envForm); results.replaceChildren(h('div', { class: 'callout info' }, 'Form filled with the docker/.env values. Save to apply them.')); });

        // ------------------------------------------------------------------ page
        const st = info.status;
        const notices = [];
        if (st.lastError) {
            notices.push(h('div', { class: 'callout error' },
                h('strong', {}, `The storage change saved ${fmtDate(st.savedAt)}${st.savedBy ? ` by ${st.savedBy}` : ''} could not be applied and was undone. `),
                `The gateway went back to the previous settings. Error: ${clip(st.lastError.message, 400)}`));
        }
        if (st.lastSwitch) {
            notices.push(h('div', { class: 'callout info' },
                `The lake moved from ${st.lastSwitch.from} to ${st.lastSwitch.to} on ${fmtDate(st.lastSwitch.at)}. Files at the old location are no longer used and can be removed.`));
        }
        if (!st.writable) notices.push(h('div', { class: 'callout error' }, `The storage config file ${st.file} cannot be written, so changes cannot be saved. In Docker, mount a directory at ${st.file.replace(/\/[^/]+$/, '')}.`));
        else if (info.persistence.configFile && !info.persistence.configFile.mounted) {
            notices.push(h('div', { class: 'callout' }, `${st.file} is not on a mounted volume, so saved settings are lost when the container is re-created. Mount a directory there (docker/compose.yaml maps ./data/config to /app/config).`));
        }
        if (!isAdmin) notices.push(h('div', { class: 'callout info' }, 'You have read-only access. Ask an admin to change storage settings.'));

        const src = (key) => h('span', { class: `badge ${info.sources[key] === 'console' ? 'console' : info.sources[key] === 'env' ? 'env' : ''}` }, SOURCE_LABEL[info.sources[key]]);

        const dataCard = h('div', { class: 'card storage-card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Data files'), h('span', { class: 'hint' }, 'In use: ', h('span', { class: 'mono' }, info.active.description), ' ', src('DUCKLAKE_DATA_PATH'))),
            h('div', { class: 'card-body' },
                h('p', { class: 'muted card-intro' }, 'Where DuckLake writes its Parquet files. The tables, their schemas and small recent writes live in the Postgres catalog below.'),
                locked ? lockedNotice(info.lake) : null,
                h('div', { class: 'choices', role: 'radiogroup', 'aria-label': 'Data file backend' }, tLocal.el, tS3.el),
                localPanel, s3Panel,
                field('Inlining row limit', inlining, 'Writes of fewer rows than this are kept in the catalog until lake maintenance flushes them to Parquet, instead of creating one small file each. 0 turns inlining off.')));

        const blobCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'BLOB storage'), src('USE_FILESYSTEM_BLOBS')),
            h('div', { class: 'card-body' },
                h('p', { class: 'muted card-intro' }, 'Binary values uploaded through the /blobs endpoints. The directory option keeps large files out of Parquet; it always uses a local or mounted directory, also when the data files are on S3.'),
                h('div', { class: 'choices', role: 'radiogroup', 'aria-label': 'BLOB storage' }, tInline.el, tFs.el),
                blobDirField, blobConfirmRow));

        const c = info.catalog;
        const catalogCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Metadata catalog (Postgres)'), h('span', { class: 'badge env' }, 'docker/.env only')),
            h('div', { class: 'card-body' },
                h('dl', { class: 'kv' },
                    h('dt', {}, 'Server'), h('dd', { class: 'mono' }, `${c.host}:${c.port}`),
                    h('dt', {}, 'Database'), h('dd', { class: 'mono' }, c.database),
                    h('dt', {}, 'User'), h('dd', { class: 'mono' }, c.user),
                    h('dt', {}, 'Password'), h('dd', {}, c.passwordSet ? '•••••••• (set)' : 'default')),
                h('p', { class: 'muted card-note' }, 'The catalog is the lake: it records every table, schema and snapshot, and also holds the console logins and settings. ' +
                    'Pointing the gateway at another Postgres opens a different lake, so this is set in docker/.env (DUCKLAKE_PG_*, POSTGRES_PASSWORD) rather than here.')));

        const retention = ctx.settings('storage').filter(s => ['SNAPSHOT_RETENTION_DAYS', 'DISABLE_DUCKLAKE_COMPACTION'].includes(s.key));
        const retentionCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Snapshots & maintenance'), h('a', { href: '#/retention', class: 'hint' }, 'Lake maintenance schedule →')),
            h('div', { class: 'card-body flush' }, settingRows(retention, isAdmin, ctx.reload)));

        const actions = isAdmin ? h('div', { class: 'card sticky-actions' }, h('div', { class: 'card-body' },
            h('div', { class: 'action-row' }, resetBtn, h('span', { class: 'spacer' }), testBtn, saveBtn),
            results,
            h('div', { class: 'muted small' }, `Saved to ${st.file}; values saved here take precedence over docker/.env.` +
                (info.autoRestart ? '' : ' This gateway does not run under a supervisor (Docker, PM2), so restart it yourself after saving.')))) : null;

        return [
            pageHeader('Storage', 'Where the lake keeps its data. Data-file and BLOB settings are applied by restarting the gateway.'),
            ...notices, dataCard, blobCard, actions, catalogCard, retentionCard,
        ];
    });

    const clip = (text, max) => (String(text).length > max ? `${String(text).slice(0, max)}…` : String(text));

    function persistenceWarning(p) {
        if (!p || p.mounted) return null;
        return h('div', { class: 'callout' }, 'This directory is not on a mounted volume: its files are lost when the container is re-created.');
    }

    function lockedNotice(lake) {
        if (lake.error) return h('div', { class: 'callout error' }, `Could not check whether the lake is empty (${lake.error}), so the data location cannot be changed right now.`);
        const names = lake.tablesWithFiles.slice(0, 8).map(t => t.table);
        return h('div', { class: 'callout' },
            h('strong', {}, 'The data location is locked. '),
            `DuckLake stores file locations relative to it, so it can only change while no table has data files, and ${lake.tablesWithFilesCount} table(s) do: `,
            h('span', { class: 'mono' }, names.join(', ') + (lake.tablesWithFilesCount > names.length ? ', …' : '')), '. ',
            'To move the lake, export these tables (Tables & Data → Export), drop them, switch, then import them again. Connection details such as credentials and the endpoint can still be changed.');
    }

    function describeChanges(a, b) {
        const out = [];
        const where = (f) => (f.backend === 's3' ? f.s3.path : f.localPath);
        if (a.backend !== b.backend || where(a) !== where(b).trim()) {
            out.push({ label: 'Data files', text: `${a.backend === 's3' ? 'S3' : 'local'} ${where(a)} → ${b.backend === 's3' ? 'S3' : 'local'} ${where(b).trim()}`, move: true });
        }
        const s3 = [['endpoint', 'endpoint'], ['region', 'region'], ['accessKey', 'access key'], ['urlStyle', 'URL style']]
            .filter(([k]) => String(a.s3[k] ?? '') !== String(b.s3[k] ?? '').trim()).map(([, l]) => l);
        if (a.s3.useSsl !== b.s3.useSsl) s3.push('TLS');
        if (b.s3.secretKey) s3.push('secret key');
        if (b.s3.sessionToken) s3.push('session token');
        if (s3.length) out.push({ label: 'S3 connection', text: `${s3.join(', ')} changed` });
        if (Number(a.inliningLimit) !== Number(b.inliningLimit)) out.push({ label: 'Inlining row limit', text: `${a.inliningLimit} → ${b.inliningLimit}` });
        if (a.blobs.mode !== b.blobs.mode || (b.blobs.mode === 'filesystem' && a.blobs.path !== b.blobs.path.trim())) {
            out.push({ label: 'BLOB storage', text: `${a.blobs.mode === 'filesystem' ? a.blobs.path : 'inside tables'} → ${b.blobs.mode === 'filesystem' ? b.blobs.path.trim() : 'inside tables'}` });
        }
        return out;
    }

    /** Wait for the gateway to go down and come back, then reload the page. */
    async function waitForRestart(target) {
        const started = Date.now();
        target.replaceChildren(h('div', { class: 'callout info' }, h('strong', {}, 'Restarting the gateway… '), 'This page reloads when it is back.'));
        let wentDown = false;
        while (Date.now() - started < 180000) {
            await new Promise(r => setTimeout(r, 1500));
            try {
                const res = await fetch('../health/ready', { cache: 'no-store' });
                if (!res.ok) { wentDown = true; continue; }
                if (wentDown || Date.now() - started > 8000) { location.reload(); return; }
            } catch { wentDown = true; }
        }
        target.replaceChildren(h('div', { class: 'callout error' }, 'The gateway has not come back after 3 minutes. Check its logs (docker compose logs app).'));
    }
})();
