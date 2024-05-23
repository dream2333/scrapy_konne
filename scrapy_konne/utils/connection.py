import asyncio
import logging
from redis.asyncio import Redis, RedisError

logger = logging.getLogger(__name__)


class GlobalRedis:
    _redis_client = None

    @classmethod
    async def from_url(self, redis_url: str) -> Redis:
        if self._redis_client is None:
            # 加全局锁，防止多次连接
            async with asyncio.Lock():
                logger.info("开始连接redis...")
                is_connected = False
                try:
                    self._redis_client = Redis.from_url(redis_url, socket_timeout=10, protocol=3)
                    is_connected = await self._redis_client.ping()
                except RedisError as e:
                    logger.error(f"Redis连接失败: {e}")
                except (ValueError, TypeError):
                    logger.error("Redis连接参数错误")
                except Exception as e:
                    logger.error(f"连接Redis出现错误: {e}")
                if not is_connected:
                    self._redis_client = None
                else:
                    logger.info("连接redis成功!")
        return self._redis_client

    @classmethod
    def close(self):
        if self._redis_client:
            self._redis_client.close()
            self._redis_client = None
            logger.info("关闭redis连接")
