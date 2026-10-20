import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import type { NatsConnection } from "nats";
import { connectNats, NatsStateStore } from "../src/nats.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY, validateContractRecord } from "../src/concord.ts";
import { canonicalJsonHash, canonicalJsonHashFromText } from "../src/json.ts";

const url = process.env.DECKR_NATS_TEST_URL;
test("real NATS preserves terms numbers, cleans list consumers, and isolates invalid records", { skip: !url }, async () => {
  const connection = await connectNats({ servers: url }) as NatsConnection;
  const manager = await connection.jetstreamManager();
  const prefix = `interop_${randomUUID().replaceAll("-", "")}`;
  let service: ConcordService | undefined;
  try {
    const contracts = await NatsStateStore.open({ connection, bucket: prefix + "_contracts" });
    const tokens = await NatsStateStore.open({ connection, bucket: prefix + "_tokens", policy: { ...CONCORD_TOKEN_STORE_POLICY, brokerTtlSeconds: 1 } });
    const terms = '{"float":1.0,"integer":9007199254740993,"negativeZero":-0.0}';
    const raw = JSON.stringify({ contractId: "numbers", generation: 1, participants: ["controller:wire"], attachedParticipants: [], state: "open", termsHash: canonicalJsonHashFromText(terms) }).slice(0, -1) + ',"terms":' + terms + '}';
    await connection.jetstream().publish(`$KV.${prefix}_contracts.contracts.numbers.1.meta`, new TextEncoder().encode(raw));
    const record = validateContractRecord((await contracts.get("contracts.numbers.1.meta"))!.value);
    assert.equal(canonicalJsonHash(record.terms!), record.termsHash);
    const consumerCount = async () => (await manager.streams.info(`KV_${prefix}_contracts`)).state.consumer_count;
    for (const filter of ["", "contracts."]) for (let i = 0; i < 5; i++) {
      assert.equal((await contracts.items(filter)).length, 1);
      assert.equal(await consumerCount(), 0);
    }
    service = new ConcordService(new ConcordCoordinator(contracts, tokens));
    const agreement = await service.ensureAgreement({ participants: ["controller:one"], localParticipant: "controller:one", localSessionId: "one" });
    assert.equal(agreement.valid, true);
    const invalid = await connection.jetstream().publish(`$KV.${prefix}_contracts.contracts.invalid.1.meta`, new TextEncoder().encode("{"));
    await service.contractSource.observed(invalid.seq);
    assert.equal(service.contractSource.snapshot.current, true);
    assert.ok(service.contractSource.get("contracts.invalid.1.meta")!.parseError);
    assert.equal(agreement.valid, true);
    for (let i = 0; i < 5; i++) { await contracts.items(); assert.equal(await consumerCount(), 1); }
    await new Promise(resolve => setTimeout(resolve, 1200));
    assert.equal(agreement.valid, true);
    assert.ok(agreement.localToken!.refreshSeq >= 2);
  } finally {
    await service?.close();
    for (const suffix of ["_contracts", "_tokens"]) await manager.streams.delete(`KV_${prefix}${suffix}`).catch(() => {});
    await connection.close();
  }
});
