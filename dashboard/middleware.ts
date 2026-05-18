// Email-whitelist middleware — Phase 02 D-20 single-user gate.
// Runs on /scan/* and /api/scan/* before request reaches the route.
import { NextRequest, NextResponse } from "next/server";
import { createServerClient, type CookieOptions } from "@supabase/ssr";

type CookieEntry = { name: string; value: string; options?: CookieOptions };

const PROTECTED_PATHS = ["/scan", "/api/scan"];

function getWhitelist(): string[] {
  return (process.env.EMAIL_WHITELIST || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

export async function middleware(req: NextRequest) {
  const isProtected = PROTECTED_PATHS.some((p) =>
    req.nextUrl.pathname.startsWith(p),
  );
  if (!isProtected) return NextResponse.next();

  const res = NextResponse.next();
  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return req.cookies.getAll();
        },
        setAll(cookiesToSet: CookieEntry[]) {
          cookiesToSet.forEach(({ name, value, options }) =>
            res.cookies.set(name, value, options),
          );
        },
      },
    },
  );

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.redirect(new URL("/login", req.url));
  }

  const whitelist = getWhitelist();
  if (whitelist.length > 0) {
    const email = (user.email || "").toLowerCase();
    if (!whitelist.includes(email)) {
      return NextResponse.redirect(
        new URL("/login?error=not_whitelisted", req.url),
      );
    }
  }

  return res;
}

export const config = {
  matcher: ["/scan/:path*", "/api/scan/:path*"],
};
