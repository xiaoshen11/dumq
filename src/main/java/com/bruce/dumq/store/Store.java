package com.bruce.dumq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.bruce.dumq.model.DuMessage;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * message store.
 * @date 2024/7/7
 */
public class Store {

    private String topic;

    public static final int LEN = 1024 * 1024 * 10;

    public Store(String topic) {
        this.topic = topic;
    }

    @Getter
    MappedByteBuffer mappedByteBuffer = null;
    @SneakyThrows
    public void init(){
        File file = new File(topic + ".dat");
        if(!file.exists()){
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ,StandardOpenOption.WRITE);
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,LEN);

        // 判断是否有数据，找到数据的结尾
        ByteBuffer byteBuffer = mappedByteBuffer.asReadOnlyBuffer();
        byte[] header = new byte[10];
        byteBuffer.get(header);
        int pos = 0;
        while (header[9] > 0){
            String json = new String(header, StandardCharsets.UTF_8).trim();
            int len = Integer.parseInt(json) + 10;
            Indexer.addEntry(topic,pos,len);
            pos += len;
            System.out.println(" next= " +pos);
            byteBuffer.position(pos);
            byteBuffer.get(header);
        }
        byteBuffer = null;
        System.out.println("init pos = " + pos);
        mappedByteBuffer.position(pos);

        // 大于10m，分成多个文件
    }

    public int pos(){
        return mappedByteBuffer.position();
    }

    public int write(DuMessage<String> dm){
        int position = mappedByteBuffer.position();
        System.out.println(" write pos -> " + position);
        String msg = JSON.toJSONString(dm);
        int len = msg.getBytes(StandardCharsets.UTF_8).length;
        String format = String.format("%010d", len);
        msg = format + msg;
        len = len +10;
        Indexer.addEntry(topic, position, len);
        mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
        return position;
    }

    public DuMessage<String> read(int offset){
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        System.out.println(" offset = " + offset);
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        readOnlyBuffer.position(entry.getOffset() + 10);
        int len = entry.getLength() - 10;
        byte[] bytes = new byte[len];
        readOnlyBuffer.get(bytes,0,len);
        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(" read json ====>> " + json);
        DuMessage<String> message = JSON.parseObject(json, new TypeReference<DuMessage<String>>() {
        });
        System.out.println(" message.body = " + message.getBody());
        return message;
    }
}
