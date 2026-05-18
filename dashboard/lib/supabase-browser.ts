// Browser-side Supabase client — used in Client Components ("use client").
// Phase 02 Plan 02-06 / D-19 / D-20 — anon key + RLS only; no service_role.
//
// Split from lib/supabase.ts because Next.js 15 App Router forbids a file that
// imports `next/headers` (server-only) from being imported by any Client
// Component. Keep server and browser factories in separate modules.
import { createBrowserClient } from "@supabase/ssr";

export function getBrowserSupabase() {
  return createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
  );
}
