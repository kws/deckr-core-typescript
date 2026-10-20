/** JSON-lines driver for the external mixed-language lifecycle test. */
import { createInterface } from "node:readline";
import type { NatsConnection } from "nats";
import { connectNats, NatsStateStore } from "../src/nats.ts";
import { ConcordCoordinator, ConcordService, CONCORD_TOKEN_STORE_POLICY } from "../src/concord.ts";
import type { ConcordAgreement, ConcordParticipantManager, ContractHandle } from "../src/concord.ts";
import { canonicalJsonHash, parseJson } from "../src/json.ts";
import type { JsonObject } from "../src/json.ts";

const [url, prefix] = process.argv.slice(2);
const endpoint = "controller:typescript", sessionId = "typescript-session";
const participants = ["controller:rust", "controller:python", endpoint];
const profile = "org.example.interop.v1";
const connection = await connectNats({ servers: url, reconnectMaxIntervalSeconds: 0.1, timeout: 200 }) as NatsConnection;
const contracts = await NatsStateStore.open({ connection, bucket: prefix + "_contracts" });
const tokens = await NatsStateStore.open({ connection, bucket: prefix + "_tokens", policy: CONCORD_TOKEN_STORE_POLICY });
const service = new ConcordService(new ConcordCoordinator(contracts, tokens));
await service.start();
let proposal: ConcordAgreement | undefined, participant: ConcordParticipantManager | undefined;
const send = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");
async function closeMode() {
  await proposal?.close(); proposal = undefined;
  await participant?.aclose(); participant = undefined;
}
async function status(contract: ContractHandle, authority: boolean) {
  const validity = await service.validate(contract);
  return { contractId: contract.contractId, generation: contract.generation, authority, valid: validity.valid,
    termsHash: validity.contract?.termsHash, calculatedHash: validity.contract?.terms ? canonicalJsonHash(validity.contract.terms) : null,
    refreshSeq: validity.tokens[endpoint]?.refreshSeq ?? 0 };
}
send({ ready: true });
try {
  for await (const line of createInterface({ input: process.stdin })) {
    const command = parseJson(line) as JsonObject;
    try {
      if (command.op === "mode") {
        await closeMode();
        if (command.mode === "participant") {
          participant = service.participantManager({ participant: endpoint, sessionId, profile, acceptContract: () => true });
          participant.start();
        }
        send({ ok: true });
      } else if (command.op === "propose") {
        proposal = await service.ensureAgreement({ participants, localParticipant: endpoint, localSessionId: sessionId, profile, terms: command.terms as JsonObject });
        send({ contractId: proposal.contract.contractId });
      } else if (command.op === "status") {
        const records = [];
        if (proposal) records.push(await status(proposal.contract, proposal.valid));
        if (participant) for (const managed of participant.managedContracts()) {
          let authority = false;
          try { await participant.authority(managed.contract).check(); authority = true; } catch { /* Terminal ownership. */ }
          records.push(await status(managed.contract, authority));
        }
        send({ current: service.contractSource.snapshot.current && service.tokenSource.snapshot.current, contracts: records });
      } else if (command.op === "counts") {
        const manager = await connection.jetstreamManager();
        send({ contracts: (await manager.streams.info(`KV_${prefix}_contracts`)).state.consumer_count,
          tokens: (await manager.streams.info(`KV_${prefix}_tokens`)).state.consumer_count });
      } else if (command.op === "consumers") {
        const manager = await connection.jetstreamManager();
        send(await manager.consumers.list(`KV_${prefix}_contracts`).next());
      } else if (command.op === "stop") { await closeMode(); send({ ok: true }); break; }
      else throw new Error("Unknown operation");
    } catch (error) { send({ error: String(error) }); }
  }
} finally { await closeMode(); await service.close(); await connection.close(); }
