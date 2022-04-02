package com.me.gmall.realtime.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: zs
 * Date: 2021/5/21
 * Desc: 用该注解修饰的属性 不需要保存到Clickhouse中，通过注解判断
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}