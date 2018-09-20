// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import org.apache.commons.lang3.ObjectUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hiral on 6/26/14.
 */
public class CollectionsUtil {

    public static <T> List<T> emptyList() {
        return (List<T>) Collections.emptyList();
    }

    public static <T, U> Map<T, U> emptyMap() {
        return (Map<T, U>) Collections.emptyMap();
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                return ObjectUtils.compare(e1.getValue(), e2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    public static <K, V> Map<K, V> getTopNByValue(Map<K, V> map, int topN, final Comparator<V> comparator) {
        Map<K, V> result = new LinkedHashMap<>();
        if (topN <= 0) {
            return result;
        }
        // min heap
        PriorityQueue<Map.Entry<K, V>> queue = new PriorityQueue<>(topN, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                return (-1) * comparator.compare(e1.getValue(), e2.getValue());
            }
        });

        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (queue.size() < topN) {
                queue.offer(entry);
            } else if (comparator.compare(queue.peek().getValue(), entry.getValue()) > 0) {
                queue.poll();
                queue.offer(entry);
            }
        }

        List<Map.Entry<K, V>> list = new ArrayList<>();
        while (!queue.isEmpty()) {
            Map.Entry<K, V> entry = queue.poll();
            list.add(entry);
        }
        // reverse
        for (int i = list.size() - 1; i >= 0; i--) {
            result.put(list.get(i).getKey(), list.get(i).getValue());
        }
        return result;
    }

    public static <K, V> Map<K, V> sortMapByValue(Map<K, V> map, final Comparator<V> comparator) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                return comparator.compare(e1.getValue(), e2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    public static <K, V1, V2> Map<K, Tuple2<V1, V2>> combine2Map(Map<K, V1> map1, Map<K, V2> map2) {
        HashSet<K> combinedKeySet = new HashSet<K>();
        Map<K, Tuple2<V1, V2>> combinedMap = new HashMap<>();
        if (map1 != null && !map1.isEmpty()) {
            combinedKeySet.addAll(map1.keySet());
        }
        if (map2 != null && !map2.isEmpty()) {
            combinedKeySet.addAll(map2.keySet());
        }
        for (K key : combinedKeySet) {
            V1 val1 = null;
            if (map1 != null) {
                val1 = map1.get(key);
            }
            V2 val2 = null;
            if (map2 != null) {
                val2 = map2.get(key);
            }
            combinedMap.put(key, new Tuple2<V1, V2>(val1, val2));
        }
        return combinedMap;
    }
}
