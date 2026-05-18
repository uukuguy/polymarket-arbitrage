// Server-side Supabase client — used in Server Components and Route Handlers.
// Phase 02 Plan 02-06 / D-19 / D-20 — anon key + RLS only; no service_role.
//
// Split from lib/supabase.ts because Next.js 15 App Router forbids a file that
// imports `next/headers` (server-only) from being imported by any Client
// Component. This file MUST NOT be imported from "use client" modules.
import { createServerClient, type CookieOptions } from "@supabase/ssr";
import { cookies } from "next/headers";

type CookieEntry = { name: string; value: string; options?: CookieOptions };

export async function getServerSupabase() {
  const cookieStore = await cookies();
  return createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll();
        },
        setAll(cookiesToSet: CookieEntry[]) {
          try {
            cookiesToSet.forEach(({ name, value, options }) =>
              cookieStore.set(name, value, options),
            );
          } catch {
            // Server Component context — can't mutate cookies; safe to ignore.
          }
        },
      },
    },
  );
}
