import os

from .app_dash import app


def main() -> int:
    host = os.getenv("DASH2_HOST", "127.0.0.1")
    port = int(os.getenv("DASH2_PORT", "8051"))
    debug = os.getenv("DASH2_DEBUG", "1").lower() in ("1", "true", "yes", "on")
    app.run(debug=debug, host=host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
