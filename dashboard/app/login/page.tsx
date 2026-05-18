// Magic-link login — Supabase Auth signInWithOtp.
// D-20: single-user whitelist enforced server-side in middleware.ts.
"use client";

import { useState } from "react";
import { getBrowserSupabase } from "@/lib/supabase";

export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<"idle" | "sending" | "sent" | "error">(
    "idle",
  );
  const [message, setMessage] = useState<string>("");

  // Surface whitelist errors set by middleware redirect.
  const errorFromQuery =
    typeof window !== "undefined"
      ? new URLSearchParams(window.location.search).get("error")
      : null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setStatus("sending");
    setMessage("");
    const supabase = getBrowserSupabase();
    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: {
        emailRedirectTo: `${window.location.origin}/auth/callback`,
      },
    });
    if (error) {
      setStatus("error");
      setMessage(error.message);
      return;
    }
    setStatus("sent");
    setMessage("Magic link sent. Check your inbox.");
  }

  return (
    <main style={{ padding: 32, maxWidth: 480 }}>
      <h1 style={{ fontSize: 24, marginBottom: 12 }}>Sign in</h1>
      <p style={{ fontSize: 13, color: "#999", marginBottom: 24 }}>
        Single-user dashboard. Only whitelisted emails can access /scan.
      </p>
      {errorFromQuery === "not_whitelisted" && (
        <div
          style={{
            background: "#3b0a0a",
            padding: 12,
            borderRadius: 4,
            marginBottom: 16,
            fontSize: 13,
          }}
        >
          That email is not on the whitelist.
        </div>
      )}
      <form onSubmit={handleSubmit}>
        <input
          type="email"
          required
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          style={{
            width: "100%",
            padding: 10,
            background: "#111",
            color: "#e5e5e5",
            border: "1px solid #333",
            borderRadius: 4,
            marginBottom: 12,
          }}
        />
        <button
          type="submit"
          disabled={status === "sending"}
          style={{
            padding: "10px 18px",
            background: "#1d4ed8",
            color: "white",
            border: 0,
            borderRadius: 4,
            cursor: "pointer",
          }}
        >
          {status === "sending" ? "Sending..." : "Send magic link"}
        </button>
      </form>
      {message && (
        <p style={{ marginTop: 16, fontSize: 13, color: "#9ec5fe" }}>
          {message}
        </p>
      )}
    </main>
  );
}
