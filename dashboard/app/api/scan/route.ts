// Vercel Edge Function — signs request body with HMAC-SHA256 and forwards to Fly /scan.
//
// BLOCKER-3 (revision 2026-05-12):
//   Vercel side env var = SCAN_SHARED_SECRET (no POLYARB_ prefix).
//   Fly side env var    = POLYARB_SCAN_SHARED_SECRET (pydantic-settings convention).
//   VALUE byte-identical on both sides.
//
// Edge Runtime constraint: must use Web Crypto API (`crypto.subtle`), NOT Node `crypto`.
// Auth precondition: dashboard/middleware.ts already gated this route on Supabase session
// + email whitelist before this handler runs.
import { NextResponse, type NextRequest } from "next/server";

export const runtime = 'edge';

export async function POST(req: NextRequest) {
  const body = await req.text();
  const secret = process.env.SCAN_SHARED_SECRET;
  const endpoint = process.env.SCAN_ENDPOINT_URL;

  if (!secret) {
    return NextResponse.json(
      { error: "server misconfigured: SCAN_SHARED_SECRET missing" },
      { status: 500 },
    );
  }
  if (!endpoint) {
    return NextResponse.json(
      { error: "server misconfigured: SCAN_ENDPOINT_URL missing" },
      { status: 500 },
    );
  }

  // HMAC-SHA256(body_bytes) with SCAN_SHARED_SECRET as key; hex-encoded digest.
  // Matches Fly daemon src/polyarb/http/scan.py validate_signature() byte-for-byte.
  const keyBytes = new TextEncoder().encode(secret);
  const bodyBytes = new TextEncoder().encode(body);
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sigBytes = await crypto.subtle.sign("HMAC", cryptoKey, bodyBytes);
  const sigHex = Array.from(new Uint8Array(sigBytes))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  let upstream: Response;
  try {
    upstream = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Signature": sigHex,
      },
      body,
    });
  } catch (e) {
    return NextResponse.json(
      {
        error: `upstream fetch failed: ${e instanceof Error ? e.message : "unknown"}`,
      },
      { status: 502 },
    );
  }

  const text = await upstream.text();
  // Pass through upstream status (200/401/422/500) and JSON content-type.
  return new NextResponse(text, {
    status: upstream.status,
    headers: { "Content-Type": "application/json" },
  });
}
