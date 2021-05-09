package com.me.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: zs
 * Date: 2021/5/9
 * Desc: 回顾SpringMVC处理流程
 * @Controller 将对象的创建交给SpringIOC容器管理
 * 注意：如果使用@Controller注解，方法返回字符串的话，会当作跳转页面的路径
 *      如果要当作普通字符串进行处理，需要在方法上加 @ResponseBody
 *      @RestController = @Controller + @ResponseBody
 * @RequestMapping  接收指定的请求，并且交给方法进行处理
 */

@RestController
public class FirstController {
    // 浏览器输出：http://localhost:8080/first?aa=zs&bb=123
    @RequestMapping("/first")
    public String first(
            @RequestParam("aa") String username,
            @RequestParam(value="bb",defaultValue = "000000") String password){
        return "hello:" + username + ":" + password;
    }
}
