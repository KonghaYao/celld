/**
 * CF matrix: PBKDF2 deriveBits — golden vector (EdgeEver-style).
 */
export async function run() {
  const password = new TextEncoder().encode("cellp-dev-edgeever");
  const salt = new Uint8Array(16);
  for (let i = 0; i < 16; i++) salt[i] = i + 1;

  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    password,
    "PBKDF2",
    false,
    ["deriveBits"],
  );

  const bits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt,
      iterations: 100_000,
      hash: "SHA-256",
    },
    keyMaterial,
    256,
  );

  const hex = [...new Uint8Array(bits)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  return { algorithm: "PBKDF2", iterations: 100_000, hash: "SHA-256", digestHex: hex };
}
