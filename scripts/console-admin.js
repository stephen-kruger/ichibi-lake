#!/usr/bin/env node
/**
 * Admin-console recovery CLI. Runs against the same DuckLake catalog as the
 * gateway (it needs the same DUCKLAKE_PG_* environment), so inside Docker:
 *
 *   docker exec ichibi-lake node scripts/console-admin.js list
 *   docker exec ichibi-lake node scripts/console-admin.js reset-password admin
 *   docker exec ichibi-lake node scripts/console-admin.js create alice --role viewer
 *   docker exec ichibi-lake node scripts/console-admin.js delete alice
 *
 * Without --password a strong random password is generated and printed.
 * Passwords passed with --password may be visible in shell history and the
 * process list; prefer the generated ones and change them after signing in.
 */
import { randomBytes } from 'crypto';
import * as store from '../src/console/store.js';
import { ROLES, hashPassword, validatePassword, validateUsername } from '../src/console/auth.js';

function usage(code = 1) {
    console.log(`Usage:
  node scripts/console-admin.js list
  node scripts/console-admin.js reset-password <username> [--password <pw>]
  node scripts/console-admin.js create <username> [--role ${ROLES.join('|')}] [--password <pw>]
  node scripts/console-admin.js delete <username>`);
    process.exit(code);
}

function option(args, name) {
    const i = args.indexOf(name);
    if (i === -1) return undefined;
    const v = args[i + 1];
    if (v === undefined || v.startsWith('--')) usage();
    return v;
}

function resolvePassword(args) {
    const supplied = option(args, '--password');
    if (supplied !== undefined) {
        const err = validatePassword(supplied);
        if (err) { console.error(err); process.exit(1); }
        return { password: supplied, generated: false };
    }
    return { password: randomBytes(15).toString('base64url'), generated: true };
}

async function main() {
    const [command, username, ...rest] = process.argv.slice(2);
    if (!command || command === '--help' || command === '-h') usage(command ? 0 : 1);

    await store.initConsoleTables();

    if (command === 'list') {
        const users = await store.listUsers();
        if (!users.length) console.log('No console logins exist yet; the first visit to /console runs setup.');
        for (const u of users) console.log(`${u.username}\t${u.role}\tlast login: ${u.last_login || 'never'}`);
        return;
    }

    if (!username) usage();
    const nameErr = validateUsername(username);
    if (nameErr) { console.error(nameErr); process.exit(1); }

    if (command === 'reset-password') {
        const user = await store.findUser(username);
        if (!user) { console.error(`No console login named "${username}".`); process.exit(1); }
        const { password, generated } = resolvePassword(rest);
        await store.updateUserPassword(user.username, await hashPassword(password));
        await store.audit('cli', 'login.update', `${user.username}: password reset via CLI`);
        console.log(`Password reset for ${user.username}.${generated ? `\nNew password: ${password}` : ''}`);
        return;
    }

    if (command === 'create') {
        const role = option(rest, '--role') || 'admin';
        if (!ROLES.includes(role)) { console.error(`Role must be one of: ${ROLES.join(', ')}`); process.exit(1); }
        if (await store.findUser(username)) { console.error(`A console login named "${username}" already exists.`); process.exit(1); }
        const { password, generated } = resolvePassword(rest);
        await store.createUser({ username, passwordHash: await hashPassword(password), role, createdBy: 'cli' });
        await store.audit('cli', 'login.create', `${username} (${role}) via CLI`);
        console.log(`Created ${role} login ${username}.${generated ? `\nPassword: ${password}` : ''}`);
        return;
    }

    if (command === 'delete') {
        const user = await store.findUser(username);
        if (!user) { console.error(`No console login named "${username}".`); process.exit(1); }
        await store.deleteUser(user.username);
        await store.audit('cli', 'login.delete', `${user.username} via CLI`);
        const remaining = await store.countUsers();
        console.log(`Deleted ${user.username}.${remaining === 0 ? ' No logins remain; the next visit to /console runs setup (the gateway log prints a new setup code).' : ''}`);
        return;
    }

    usage();
}

main()
    .then(() => process.exit(0))
    .catch((err) => { console.error(err.message || err); process.exit(1); });
