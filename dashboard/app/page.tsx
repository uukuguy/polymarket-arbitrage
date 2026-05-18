// Root index — redirects to /status (the primary L1 timeline view).
import { redirect } from "next/navigation";

export default function HomePage() {
  redirect("/status");
}
