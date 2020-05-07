import asyncio

import uvloop

from .service import main

if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
