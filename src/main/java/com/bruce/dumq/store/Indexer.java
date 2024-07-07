package com.bruce.dumq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * entry indexer.
 * @date 2024/7/7
 */
public class Indexer {

    static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();
    static Map<Integer, Entry> mappings = new HashMap<>();

    @AllArgsConstructor
    @Data
    public static class Entry{
        int offset;
        int length;

    }

    public static void addEntry(String topic, int offset, int length){
        Entry entry = new Entry( offset, length);
        indexes.add(topic, entry);
        mappings.put(offset,entry);
    }

    public static List<Entry> getEntries(String topic){
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic,int offset){
        return mappings.get(offset);
    }
}
