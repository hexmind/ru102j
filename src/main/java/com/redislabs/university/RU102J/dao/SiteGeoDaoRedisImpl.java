package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Coordinate;
import com.redislabs.university.RU102J.api.GeoQuery;
import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.*;

import java.util.*;
import java.util.stream.Collectors;

public class SiteGeoDaoRedisImpl implements SiteGeoDao {
    private JedisPool jedisPool;
    final static private Double capacityThreshold = 0.2;

    public SiteGeoDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Site findById(long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields =
                    jedis.hgetAll(RedisSchema.getSiteHashKey(id));
            if (fields == null || fields.isEmpty()) {
                return null;
            }
            return new Site(fields);
        }
    }

    @Override
    public Set<Site> findAll() {
        return findAllHashes().stream()
                .map(Response::get)
                .filter(hash -> !hash.isEmpty())
                .map(Site::new)
                .collect(Collectors.toSet());
    }

    private Set<Response<Map<String, String>>> findAllHashes() {
        try (Jedis jedis = jedisPool.getResource();
             Pipeline pipe = jedis.pipelined()) {
            Set<String> keys = jedis.zrange(RedisSchema.getSiteGeoKey(), 0, -1);
            return keys.stream()
                    .map(pipe::hgetAll)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public Set<Site> findByGeo(GeoQuery query) {
        if (query.onlyExcessCapacity()) {
            return findSitesByGeoWithCapacity(query);
        } else {
            return findSitesByGeo(query);
        }
    }

    private Set<Site> findSitesByGeoWithCapacity(GeoQuery query) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<Site> sites = getGeoRadii(jedis, query).stream()
                    .map(response -> jedis.hgetAll(response.getMemberByString()))
                    .filter(Objects::nonNull)
                    .map(Site::new).collect(Collectors.toSet());

            final Map<Long, Response<Double>> scores = getCapacities(jedis, sites);

            return sites.stream()
                    .filter(site -> scores.get(site.getId()).get() >= capacityThreshold)
                    .collect(Collectors.toSet());
        }
    }

    private List<GeoRadiusResponse> getGeoRadii(Jedis jedis, GeoQuery query) {
        return jedis.georadius(
                RedisSchema.getSiteGeoKey(),
                query.getCoordinate().lng,
                query.getCoordinate().lat,
                query.getRadius(),
                query.getRadiusUnit());
    }

    private Map<Long, Response<Double>> getCapacities(Jedis jedis, Set<Site> sites) {
        String key = RedisSchema.getCapacityRankingKey();
        try (Pipeline pipeline = jedis.pipelined()) {
            return sites.stream().collect(Collectors.toMap(
                    Site::getId,
                    s -> pipeline.zscore(key, String.valueOf(s.getId()))
            ));
        }
    }

    private Set<Site> findSitesByGeo(GeoQuery query) {
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

        try (Jedis jedis = jedisPool.getResource()) {
            List<GeoRadiusResponse> radiusResponses =
                    jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                            coord.getLat(), radius, radiusUnit);

            return radiusResponses.stream()
                    .map(response -> jedis.hgetAll(response.getMemberByString()))
                    .filter(Objects::nonNull)
                    .map(Site::new).collect(Collectors.toSet());
        }
    }

    @Override
    public void insert(Site site) {
         try (Jedis jedis = jedisPool.getResource()) {
             String key = RedisSchema.getSiteHashKey(site.getId());
             jedis.hmset(key, site.toMap());

             if (site.getCoordinate() == null) {
                 throw new IllegalArgumentException("Coordinate required for Geo " +
                         "insert.");
             }
             Double longitude = site.getCoordinate().getGeoCoordinate().getLongitude();
             Double latitude = site.getCoordinate().getGeoCoordinate().getLatitude();
             jedis.geoadd(RedisSchema.getSiteGeoKey(), longitude, latitude,
                     key);
         }
    }
}
