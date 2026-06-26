#!/usr/bin/env node
/**
 * SuperLake Wizard — generate Docker Compose configs that link multiple
 * ichibi-lake DuckLake instances into one logical lakehouse.
 *
 * Starts from the vanilla `docker/` config as a baseline and writes the
 * generated artefacts under `superlake/<option-slug>/`. The `docker/` and
 * `docker-tailscale/` directories are never modified.
 *
 * Run with:  node scripts/superlake-wizard.js
 *      or:  npm run superlake
 */

import { banner, section, c, promptChoice, closePrompts } from './superlake/prompts.js';
import { summary } from './superlake/util.js';

import option1 from './superlake/option1-singlehost.js';
import option2 from './superlake/option2-nfs.js';
import option3 from './superlake/option3-minio.js';
import option4 from './superlake/option4-sshfs.js';
import option5 from './superlake/option5-federation.js';

const OPTIONS = [
    {
        label: 'Single-host scale-out  (N gateways share one bind-mount + one Postgres)',
        run:   option1,
    },
    {
        label: 'Multi-host NFS share    (one storage host exports /srv/ichibi-lake)',
        run:   option2,
    },
    {
        label: 'Multi-host MinIO        (self-hosted S3 API on your LAN)',
        run:   option3,
    },
    {
        label: 'Multi-host SSHFS        (FUSE mount over SSH, no extra services)',
        run:   option4,
    },
    {
        label: 'Federation              (per-host lakes, cross-attached for queries)',
        run:   option5,
    },
];

async function main() {
    banner(
        'SuperLake Wizard',
        'Compose-file generator for multi-node DuckLake topologies'
    );

    console.log(c.dim(
        'This wizard never modifies docker/ or docker-tailscale/.\n' +
        'Output is written under superlake/<option-slug>/ in this repo.\n'
    ));

    section('Pick a topology');
    const choice = await promptChoice(
        'Which configuration do you want to generate?',
        OPTIONS.map(o => o.label),
        1,
    );

    console.log('');
    section(`Configuring: ${OPTIONS[choice - 1].label}`);

    let result;
    try {
        result = await OPTIONS[choice - 1].run();
    } finally {
        closePrompts();
    }

    if (!result) {
        console.log(c.yellow('\nWizard exited without generating files.'));
        return;
    }

    summary('Generation complete', [
        c.bold('Output directory: ') + c.cyan(result.outDir.rel),
        '',
        c.bold('Files written:'),
        ...result.files.map(f => '  • ' + f),
        '',
        c.bold('Next steps:'),
        ...result.nextSteps.map(s => '  ' + s),
        '',
        c.dim('Re-run this wizard at any time:  npm run superlake'),
    ]);
}

main().catch(err => {
    closePrompts();
    console.error('\n' + c.red('Wizard failed: ') + (err?.stack || err?.message || err));
    process.exit(1);
});
