/**
 * CF matrix: Ed25519 (and NODE-ED25519 spelling) sign → verify.
 */
export async function run() {
  const data = new TextEncoder().encode("celld-ed25519");
  const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ]);
  const signature = await crypto.subtle.sign("Ed25519", pair.privateKey, data);
  const ok = await crypto.subtle.verify(
    "Ed25519",
    pair.publicKey,
    signature,
    data,
  );
  const forged = new Uint8Array(signature);
  forged[0] ^= 0x01;
  const reject = await crypto.subtle.verify(
    "Ed25519",
    pair.publicKey,
    forged,
    data,
  );
  return {
    algorithm: "Ed25519",
    ok,
    rejectForged: reject === false,
    signatureBytes: signature.byteLength,
  };
}
