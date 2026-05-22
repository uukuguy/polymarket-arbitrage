"""Probe: 在 daemon 进程内手动 capture 一次 exception 把 Sentry SDK
breadcrumb buffer 上传到 Sentry server, 用于验证 Phase 02.1 Plan 01
must_haves.truth 2 (breadcrumb 出现在 Sentry event 上).

如何跑 (在生产容器内):
  flyctl ssh console -a polyarb-l1
  cd /app
  python scripts/sentry_breadcrumb_probe.py

如果不想 ssh 进 shell, 用 sftp/scp 一行版:
  flyctl ssh sftp shell -a polyarb-l1 << 'EOF'
  put scripts/sentry_breadcrumb_probe.py /tmp/probe.py
  EOF
  flyctl ssh console -a polyarb-l1 -C 'cd /app && python /tmp/probe.py'

期待输出: "probe sent — check Sentry for new issue titled
'Inj 3-v2 breadcrumb verification probe (Phase 02.1)'"

会触发的 Sentry event 会带:
- 最近一次 snapshot 的 mirror breadcrumb (如果 mirror_enabled=False 路径
  在本次 daemon 进程生命周期内跑过 step 7.5)
- 本脚本主动加的 verification-probe breadcrumb (定位锚)
"""

from __future__ import annotations

import sys

import sentry_sdk

from polyarb.config import load_settings
from polyarb.observability.sentry import init_sentry


def main() -> int:
    settings = load_settings()
    init_sentry(settings)

    sentry_sdk.add_breadcrumb(
        category="verification-probe",
        level="info",
        message="Inj 3-v2 breadcrumb buffer probe",
        data={
            "phase": "02.1",
            "plan": "01",
            "truth": "mirror breadcrumb present on prod Sentry event",
        },
    )

    try:
        raise RuntimeError(
            "Inj 3-v2 breadcrumb verification probe (Phase 02.1) — "
            "intentional, safe to ignore; verifies breadcrumb upload path"
        )
    except RuntimeError:
        sentry_sdk.capture_exception()

    # 强制 flush, Sentry SDK 默认是后台线程异步发送, 短脚本退太快会丢
    sentry_sdk.flush(timeout=5.0)
    print(
        "probe sent — check Sentry for new issue titled "
        "'Inj 3-v2 breadcrumb verification probe (Phase 02.1)'"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
