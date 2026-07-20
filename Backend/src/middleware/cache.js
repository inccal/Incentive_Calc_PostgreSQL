import NodeCache from "node-cache";

const cache = new NodeCache({ stdTTL: 300 });

export const cacheMiddleware = (duration = 300) => (req, res, next) => {
  if (req.method !== "GET") {
    return next();
  }

  // Authenticated list endpoints can return different data for the same URL.
  // Include identity and role so one Head can never receive another Head's cache.
  const identity = req.user?.id || "anonymous";
  const role = req.user?.role || "anonymous";
  const key = `__express__${identity}:${role}:${req.originalUrl || req.url}`;
  const cachedResponse = cache.get(key);

  if (cachedResponse) {
    return res.json(cachedResponse);
  }

  const originalJson = res.json;
  res.json = (body) => {
    res.originalJson = res.json;
    originalJson.call(res, body);
    cache.set(key, body, duration);
  };
  next();
};

export const clearCacheMiddleware = (req, res, next) => {
    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
        cache.flushAll();
    }
    next();
};
