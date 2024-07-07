package com.bruce.dumq.server;

import com.bruce.dumq.model.DuMessage;
import com.bruce.dumq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @date 2024/7/1
 */
@RestController
@RequestMapping("/dumq")
public class MQServer {

    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody() DuMessage<String> message){
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    @RequestMapping("/recv")
    public Result<DuMessage<?>> recv(@RequestParam("t") String topic,
                                          @RequestParam("cid")String consumerId){
        return Result.msg(MessageQueue.recv(topic,consumerId));
    }

    @RequestMapping("/batchRecv")
    public Result<List<DuMessage<?>>> batchRecv(@RequestParam("t") String topic,
                                                    @RequestParam("cid")String consumerId,
                                                    @RequestParam(value = "size",required = false,defaultValue = "1000")Integer size){
        return Result.msg(MessageQueue.batchRecv(topic,consumerId,size));
    }

    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid")String consumerId,
                              @RequestParam("offset")Integer offset){
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    @RequestMapping("/sub")
    public Result<String> sub(@RequestParam("t") String topic,
                                    @RequestParam("cid")String consumerId){
        MessageQueue.sub(new MessageSubscription(topic,consumerId,-1));
        return Result.ok();
    }

    @RequestMapping("/sunub")
    public Result<String> unsub(@RequestParam("t") String topic,
                                      @RequestParam("cid")String consumerId){
        MessageQueue.unsub(new MessageSubscription(topic,consumerId,-1));
        return Result.ok();
    }

}
