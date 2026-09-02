/**
 * CF matrix: RSA-OAEP encrypt → decrypt (celld host ops).
 * Ciphertext is OAEP-randomized; only the recovered plaintext is asserted.
 */
export async function run() {
  const pair = await crypto.subtle.generateKey(
    {
      name: "RSA-OAEP",
      modulusLength: 2048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: "SHA-256",
    },
    true,
    ["encrypt", "decrypt"],
  );
  const plaintext = new TextEncoder().encode("celld-oaep");
  const ciphertext = await crypto.subtle.encrypt(
    { name: "RSA-OAEP" },
    pair.publicKey,
    plaintext,
  );
  const recovered = await crypto.subtle.decrypt(
    { name: "RSA-OAEP" },
    pair.privateKey,
    ciphertext,
  );
  const text = new TextDecoder().decode(recovered);
  return {
    algorithm: "RSA-OAEP",
    hash: "SHA-256",
    ok: text === "celld-oaep",
    cipherBytes: ciphertext.byteLength,
  };
}
