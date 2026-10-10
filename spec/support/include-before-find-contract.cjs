'use strict';
const assert = require('assert/strict');
assert.equal(global.__STRESS_LOCAL_EGRESS_GUARD_INSTALLED__, true, 'Preload the local egress guard first');
const fs = require('fs');
const os = require('os');
const path = require('path');
const net = require('net');
const { spawn } = require('child_process');
const express = require('express');
const { MongoClient } = require('mongodb');
const Parse = require('parse/node');
const ParseServer = require('../../lib/ParseServer').default;
const binary = process.env.INCLUDE_CONTRACT_MONGOD;
assert.ok(binary && path.isAbsolute(binary), 'Explicit existing mongod binary required');
fs.accessSync(binary, fs.constants.X_OK);
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'parse-include-contract-'));
const evidence = { node: process.version, started: new Date().toISOString(), directory, clpScope: 'Class-level permissions are not independently tested by this contract', modes: [] };
const resources = [];
let mongo;
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const listen = server => new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', () => resolve(server.address().port));
});
const close = server => new Promise(resolve => server.close(resolve));
async function denied(response) { assert.ok(response.status >= 400); assert.equal((await response.json()).code, 119); }
async function mode(option) {
  const app = express();
  const listener = app.listen(0, '127.0.0.1');
  await new Promise(resolve => listener.once('listening', resolve));
  const port = listener.address().port;
  const counts = { parent: 0, child: 0 };
  const options = {
    appId: 'include-contract-' + String(option), masterKey: 'synthetic-contract-master', restAPIKey: 'synthetic-contract-rest',
    serverURL: `http://127.0.0.1:${port}/parse`, databaseURI: `mongodb://127.0.0.1:${mongo.port}/include_${String(option)}`,
    allowClientClassCreation: true, masterKeyIps: ['127.0.0.1'], verifyServerUrl: false,
    protectedFields: { ContractChild: { '*': ['secret'] } },
    logLevels: { triggerBeforeError: 'silent', triggerBeforeSuccess: 'silent', triggerAfter: 'silent' },
    silent: true,
    cloud() {
      Parse.Cloud.beforeFind('ContractChild', request => { if (!request.master) throw new Parse.Error(119, 'Contract direct read denied'); });
      Parse.Cloud.afterFind('ContractChild', request => { if (!request.master) counts.child++; return request.objects; });
      Parse.Cloud.afterFind('ContractParent', request => { if (!request.master) counts.parent++; return request.objects; });
    }
  };
  if (option !== undefined) options.enableBeforeFindOnInclude = option;
  const api = new ParseServer(options);
  api.server = listener;
  resources.push({ api, listener });
  assert.equal(api.config.enableBeforeFindOnInclude, option !== false);
  await api.start();
  app.use('/parse', api.app);
  const request = (route, method = 'GET', body, master = false) => fetch(options.serverURL + route, {
    method, signal: AbortSignal.timeout(5000), headers: { 'Content-Type': 'application/json', 'X-Parse-Application-Id': options.appId,
      'X-Parse-REST-API-Key': options.restAPIKey, ...(master ? { 'X-Parse-Master-Key': options.masterKey } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) })
  });
  const save = async (className, body) => { const response = await request('/classes/' + className, 'POST', body, true); assert.equal(response.status, 201); return (await response.json()).objectId; };
  const visible = await save('ContractChild', { name: 'public synthetic child', secret: 'synthetic private field', ACL: { '*': { read: true } } });
  const privateId = await save('ContractChild', { name: 'private synthetic child', secret: 'synthetic private field', ACL: {} });
  const parent = await save('ContractParent', { child: { __type: 'Pointer', className: 'ContractChild', objectId: visible }, ACL: { '*': { read: true } } });
  const privateParent = await save('ContractParent', { child: { __type: 'Pointer', className: 'ContractChild', objectId: privateId }, ACL: { '*': { read: true } } });
  await denied(await request('/classes/ContractChild/' + visible));
  await denied(await request('/classes/ContractChild?where=' + encodeURIComponent(JSON.stringify({ objectId: visible }))));
  const included = await request('/classes/ContractParent?include=child&where=' + encodeURIComponent(JSON.stringify({ objectId: parent })));
  if (option !== false) await denied(included);
  else {
    assert.equal(included.status, 200);
    const child = (await included.json()).results[0].child;
    assert.equal(child.__type, 'Object'); assert.equal(child.objectId, visible);
    assert.equal(child.name, 'public synthetic child'); assert.equal(child.secret, undefined);
    assert.ok(counts.parent > 0 && counts.child > 0, 'Both afterFind hooks must remain active');
    const privateResult = await request('/classes/ContractParent?include=child&where=' + encodeURIComponent(JSON.stringify({ objectId: privateParent })));
    assert.equal(privateResult.status, 200);
    assert.equal((await privateResult.json()).results[0].child, undefined, 'Private ACL must still block expansion');
  }
  evidence.modes.push({ option: option === undefined ? 'omitted' : option, effective: api.config.enableBeforeFindOnInclude, directGetDenied: true, directFindDenied: true,
    includeDenied: option !== false, parentAfterFind: counts.parent, childAfterFind: counts.child, ...(option === false ? { protectedFieldRemoved: true, privateAclRespected: true } : {}) });
  await api.handleShutdown(); resources.pop();
}
(async () => {
  const reserve = net.createServer();
  const port = await listen(reserve); await close(reserve);
  fs.mkdirSync(path.join(directory, 'db'));
  const child = spawn(binary, ['--bind_ip', '127.0.0.1', '--port', String(port), '--dbpath', path.join(directory, 'db'), '--logpath', path.join(directory, 'mongod.log'), '--nounixsocket'], { env: process.env, stdio: 'ignore' });
  mongo = { child, port };
  evidence.mongoPort = port;
  let ready = false;
  for (let attempt = 0; attempt < 40 && !ready; attempt++) {
    const client = new MongoClient(`mongodb://127.0.0.1:${port}`, { serverSelectionTimeoutMS: 200 });
    try { await client.connect(); ready = true; } catch { await wait(100); } finally { await client.close(); }
  }
  assert.ok(ready, 'Task-owned MongoDB failed to become ready');
  const invalid = { appId: 'invalid-include-contract', masterKey: 'synthetic-invalid-master', serverURL: 'http://127.0.0.1:1/parse', enableBeforeFindOnInclude: 'false' };
  assert.throws(() => new ParseServer(invalid), /enableBeforeFindOnInclude must be a boolean/);
  evidence.invalidBooleanRejected = true;
  for (const option of [undefined, true, false]) await mode(option);
  evidence.ok = true;
})().catch(error => { evidence.ok = false; evidence.failure = { name: error.name, message: error.message }; process.exitCode = 1; }).finally(async () => {
  for (const resource of resources.reverse()) { await resource.api.handleShutdown().catch(() => close(resource.listener)); }
  if (mongo?.child.exitCode === null) { mongo.child.kill('SIGTERM'); await new Promise(resolve => mongo.child.once('exit', resolve)); }
  evidence.finished = new Date().toISOString();
  fs.writeFileSync(path.join(directory, 'contract-result.json'), JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify(evidence, null, 2));
  process.exit(process.exitCode || 0);
});
