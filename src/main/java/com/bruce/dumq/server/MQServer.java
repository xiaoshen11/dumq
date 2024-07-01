package com.bruce.dumq.server;

import com.bruce.dumq.model.DuMessage;
import com.bruce.dumq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @date 2024/7/1
 */
@Controller
@RequestMapping("/dumq")
public class MQServer {

    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestParam("cid")String consumerId,
                               @RequestBody() DuMessage<String> message){
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    @RequestMapping("/recv")
    public Result<DuMessage<String>> recv(@RequestParam("t") String topic,
                                          @RequestParam("cid")String consumerId){
        return Result.msg(MessageQueue.recv(topic,consumerId));
    }

    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid")String consumerId,
                              @RequestParam("offset")Integer offset){
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid")String consumerId){
        MessageQueue.sub(new MessageSubscription(topic,consumerId,-1));
        return Result.ok();
    }

    @RequestMapping("/sunub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid")String consumerId){
        MessageQueue.unsub(new MessageSubscription(topic,consumerId,-1));
        return Result.ok();
    }

}
