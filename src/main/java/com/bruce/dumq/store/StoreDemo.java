package com.bruce.dumq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.bruce.dumq.model.DuMessage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

/**
 * @date 2024/7/7
 */
public class StoreDemo {

    public static void main(String[] args) throws IOException {
        String content = """
                this is a good file.
                that is a new line for store.
                """;
        int length = content.getBytes(StandardCharsets.UTF_8).length;
        System.out.println(" len = " + length);
        File file = new File("test.dat");
        if(!file.exists()){
         file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        try(FileChannel channel = (FileChannel)Files.newByteChannel(path,
                StandardOpenOption.READ,StandardOpenOption.WRITE)){
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,1024);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + " -> " + mappedByteBuffer.position());
                DuMessage<String> dm = DuMessage.create(i + ":"+content,null);
                String jsonString = JSON.toJSONString(dm);
                Indexer.addEntry("test",mappedByteBuffer.position(), jsonString.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(jsonString));
            }

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while(sc.hasNext()){
                String line = sc.nextLine();
                if(line.equals('q')){
                    break;
                }
                System.out.println(" IN = " + line);
                int id = Integer.parseInt(line);
                Indexer.Entry entry = Indexer.getEntry("test", id);

                readOnlyBuffer.position(entry.getOffset());
                int len = entry.getLength();
                byte[] bytes = new byte[len];
                readOnlyBuffer.get(bytes,0,len);
                String s = new String(bytes, StandardCharsets.UTF_8);
                System.out.println(" read only ====>> " + s);
                DuMessage<String> message = JSON.parseObject(s, new TypeReference<DuMessage<String>>() {
                });
                System.out.println(" message.body = " + message.getBody());
            }

        }


    }

}
