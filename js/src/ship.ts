// hook-sync — HTTP ship with ACK

import type { Change, SyncResponse } from "./types.ts";

/**
 * Ship a batch of changes to a peer and return the ACK response.
 * Uses fetch() — available in both Bun and Node 18+.
 * Returns null on connection error or non-200 response.
 */
export async function shipWithAck(
	nodeId: string,
	batchId: number,
	changes: Change[],
	peerUrl: string,
): Promise<SyncResponse | null> {
	if (changes.length === 0) return { applied: 0, ack: batchId };

	try {
		const resp = await fetch(`${peerUrl}/sync`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-Node-Id": nodeId },
			body: JSON.stringify({ batch_id: batchId, changes }),
		});
		if (!resp.ok) return null;
		return (await resp.json()) as SyncResponse;
	} catch {
		return null;
	}
}
