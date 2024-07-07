package com.bruce.dumq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.bruce.dumq.model.DuMessage;
import com.bruce.dumq.model.Result;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * broker for topics.
 * @date 2024/6/25
 */
public class DuBroker {

    @Getter
    public static DuBroker Default = new DuBroker();

    public static String brokerUrl = "http://localhost:8765/dumq";

    static {
        init();
    }

    private static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, DuConsumer> consumers1 = Default.getConsumers();
            consumers1.forEach((topic,consumers2) -> {
                consumers2.forEach(consumer -> {
                    DuMessage recv = consumer.recv(topic);
                    if(recv == null || recv.getBody() == null){
                        return;
                    }
                    try {
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic,recv);
                    }catch (Exception e){
                        //TODO

                    }
                });
            });

        },100,100);

    }


    public DuProducer createProduce(){
        return new DuProducer(this);
    }

    public DuConsumer<?> createConsumer(String topic){
        DuConsumer<?> consumer = new DuConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, DuMessage message) {
        System.out.println(" ====>> send message/topic: " + topic + "/" + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>(){});
        System.out.println(" ====>> send result: " + result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        System.out.println(" ====>> sub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic +"&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ====>> sub result: " + result);
    }

    public <T> DuMessage<T> recv(String topic, String cid) {
        System.out.println(" ====>> recv topic/cid: " + topic + "/" + cid);
        Result<DuMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic +"&cid=" + cid,
                new TypeReference<Result<DuMessage<String>>>(){});
        System.out.println(" ====>> recv result: " + result);
        return (DuMessage<T>)result.getData();
    }

    public void unsub(String topic, String cid) {
        System.out.println(" ====>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic +"&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ====>> unsub result: " + result);
    }

    public boolean ack(String topic, String cid, Integer offset){
        System.out.println(" ====>> ack topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic +"&cid=" + cid+"&offset=" + offset,
                new TypeReference<Result<String>>(){});
        System.out.println(" ====>> ack result: " + result);
        return result.getCode() == 1;
    }

    @Getter
    private MultiValueMap<String, DuConsumer> consumers = new LinkedMultiValueMap();
    public void addConsumer(String topic, DuConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }
}
